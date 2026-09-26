"""Pipeline stage extracted from 04_join_transform.ipynb.
Existing notebook logic is preserved inside run_join_transform().
"""

def run_join_transform():
    # ============================================================
    # CELL  — gold transformation (INCREMENTAL → external Delta tables)
    # ============================================================
    # Databricks notebook: gold_join_transform (incremental)
    #
    # Transformation logic (PARTS 1-8) runs on the FULL silver data —
    # needed so team_lookup / match_lookup stay correct. Only rows not
    # already in gold are written, using MERGE (upsert).
    #
    # Tables are EXTERNAL Delta tables: each one lives at an explicit,
    # readable path inside the gold container (gold/delta_tables/<name>),
    # registered in Unity Catalog. The schema deliberately has NO managed
    # location — UC forbids external tables inside a managed location.
    #
    # Output tables:
    #   1. match_stats   — fact table; one row per team per match
    #   2. teams         — team master table
    #   3. player_stats  — one row per player per match
    #   4. match_events  — event-level table (ESPN IDs crosswalked)
    #   5. weather       — one row per matched fixture
    #   6. h2h           — historical head-to-head lookup
    #
    # Input:  silver/fixtures, silver/match_stats, silver/player_stats,
    #         silver/match_events, silver/weather, silver/h2h
    # Output: dbw_spl_pipeline.spl_gold.<table>
    #         → abfss://gold@splpipelinestorage.../delta_tables/<table>

    import io
    import pandas as pd
    from azure.storage.filedatalake import DataLakeServiceClient

    # --- Storage access (Azure SDK — used for reading silver CSVs) ---
    storage_account = "splpipelinestorage"

    storage_key = dbutils.secrets.get(scope="spl-scope", key="storage-account-key")

    service_client = DataLakeServiceClient(
        account_url=f"https://{storage_account}.dfs.core.windows.net",
        credential=storage_key,
    )

    silver_fs = service_client.get_file_system_client(file_system="silver")

    # --- Gold Delta tables — external, inside the gold container ---
    # Spark reaches the storage through the Unity Catalog external location
    # (ext-spl-gold → cred-spl-storage → ac-spl-pipeline managed identity),
    # so no storage key is passed to Spark here.
    GOLD_CATALOG = "dbw_spl_pipeline"
    GOLD_SCHEMA_NAME = "spl_gold"
    GOLD_LOCATION = f"abfss://gold@{storage_account}.dfs.core.windows.net/delta_tables"

    # No MANAGED LOCATION here on purpose: every table below is external
    # and sets its own LOCATION, which UC rejects inside a managed path.
    spark.sql(f"""
        CREATE SCHEMA IF NOT EXISTS {GOLD_CATALOG}.{GOLD_SCHEMA_NAME}
    """)
    GOLD_SCHEMA = f"{GOLD_CATALOG}.{GOLD_SCHEMA_NAME}"
    print(f"Gold tables      → {GOLD_SCHEMA}")
    print(f"Physical storage → {GOLD_LOCATION}/<table_name>")

    # Which gold tables already exist. SHOW TABLES is used instead of
    # spark.catalog.tableExists() — the latter doesn't resolve 3-part
    # catalog.schema.table names on serverless and always returns False,
    # which would make every run recreate the tables from scratch.
    existing_gold_tables = {
        row["tableName"] for row in spark.sql(f"SHOW TABLES IN {GOLD_SCHEMA}").collect()
    }
    print("Existing gold tables:", sorted(existing_gold_tables) or "none yet")


    def read_csv_from_silver(path: str) -> pd.DataFrame:
        file_client = silver_fs.get_file_client(path)
        content = file_client.download_file().readall()
        return pd.read_csv(io.BytesIO(content))


    def get_existing_ids(table_name: str, id_columns: list) -> set:
        """Key values already saved in a gold Delta table.
        Empty set if the table doesn't exist yet (first run = full load)."""
        if table_name not in existing_gold_tables:
            return set()
        full_name = f"{GOLD_SCHEMA}.{table_name}"
        existing = spark.table(full_name).select(*id_columns).distinct().toPandas()
        if len(id_columns) == 1:
            return set(existing[id_columns[0]])
        return set(existing.itertuples(index=False, name=None))


    def upsert_to_gold(df: pd.DataFrame, table_name: str, merge_keys: list) -> None:
        """Upsert a pandas DataFrame into a gold Delta table.
        Creates the table as EXTERNAL on first run, at an explicit path.
        SQL MERGE is used so this works on serverless, classic and shared
        compute alike."""
        if df.empty:
            print(f"{table_name}: no new rows — skipped.")
            return

        full_name = f"{GOLD_SCHEMA}.{table_name}"
        spark_df = spark.createDataFrame(df)

        if table_name not in existing_gold_tables:
            (
                spark_df.write.format("delta")
                .mode("overwrite")
                .option("path", f"{GOLD_LOCATION}/{table_name}")
                .saveAsTable(full_name)
            )
            existing_gold_tables.add(table_name)
            print(f"{table_name}: table created — {df.shape[0]:,} rows.")
            return

        temp_view = f"src_{table_name}"
        spark_df.createOrReplaceTempView(temp_view)
        on_clause = " AND ".join(f"t.{k} = s.{k}" for k in merge_keys)
        spark.sql(f"""
            MERGE INTO {full_name} AS t
            USING {temp_view} AS s
            ON {on_clause}
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)
        print(f"{table_name}: merged {df.shape[0]:,} new/updated rows.")


    # ============================================================
    # PART 0 — Load all six silver tables + read the gold watermark
    # ============================================================

    fixtures = read_csv_from_silver("fixtures/cleaned_fixtures.csv")
    stats = read_csv_from_silver("match_stats/cleaned_stats.csv")
    player_stats = read_csv_from_silver("player_stats/cleaned_player_stats.csv")
    match_events = read_csv_from_silver("match_events/cleaned_match_events.csv")
    weather = read_csv_from_silver("weather/cleaned_weather.csv")
    h2h = read_csv_from_silver("h2h/cleaned_h2h.csv")

    print("Fixtures:", fixtures.shape)
    print("Stats:", stats.shape)
    print("Player Stats:", player_stats.shape)
    print("Match Events:", match_events.shape)
    print("Weather:", weather.shape)
    print("H2H:", h2h.shape)

    # What's already in gold — drives the filtering in PART 9.
    existing_match_ids = get_existing_ids("match_stats", ["match_id"])
    existing_team_ids = get_existing_ids("teams", ["team_id"])
    existing_event_ids = get_existing_ids("match_events", ["event_id"])
    existing_h2h_ids = get_existing_ids("h2h", ["h2h_match_id"])
    print(f"Gold watermark — matches already saved: {len(existing_match_ids)}")


    # ============================================================
    # PART 1 — Standardize team naming
    # ============================================================
    # Note: team_id 2956's "Dhamk" → "Damac" naming fix happens in
    # silver_fixtures_stats.py (a raw-value correction belongs at the
    # per-source cleaning stage, not here in gold).

    home_teams = fixtures[["teams_home_id", "teams_home_name"]].rename(
        columns={"teams_home_id": "team_id", "teams_home_name": "team"}
    )
    away_teams = fixtures[["teams_away_id", "teams_away_name"]].rename(
        columns={"teams_away_id": "team_id", "teams_away_name": "team"}
    )
    team_lookup = pd.concat([home_teams, away_teams], ignore_index=True).drop_duplicates()

    team_name_conflicts = (
        team_lookup.groupby("team")["team_id"].nunique().loc[lambda s: s > 1]
    )
    if not team_name_conflicts.empty:
        raise ValueError(f"Team names map to multiple team IDs: {team_name_conflicts.to_dict()}")

    stats = stats.drop(columns=["team_id"], errors="ignore")
    stats = stats.merge(team_lookup, on="team", how="left", validate="many_to_one")

    if stats["team_id"].isna().any():
        missing_teams = stats.loc[stats["team_id"].isna(), "team"].unique().tolist()
        raise ValueError(f"Could not map team_id for: {missing_teams}")

    print("Team IDs successfully added to Stats.")


    # ============================================================
    # PART 2 — Build Match Stats (fact table)
    # ============================================================
    # Grain: one row = one team's statistics in one match.
    # PK: match_id + team_id

    match_stats = fixtures.merge(stats, on="fixture_id", how="left", validate="one_to_many")

    match_stats = match_stats.rename(columns={
        "fixture_id": "match_id",
        "team": "team_name",
        "passes_%": "passes_pct",
    })

    duplicate_match_stats_pk = match_stats.duplicated(["match_id", "team_id"]).sum()
    if duplicate_match_stats_pk != 0:
        raise ValueError(f"Match Stats has {duplicate_match_stats_pk} duplicate (match_id, team_id) keys.")

    # Sort chronologically (oldest match first, home/away grouped by match).
    match_stats["fixture_date"] = pd.to_datetime(match_stats["fixture_date"], errors="coerce")
    match_stats = match_stats.sort_values(["fixture_date", "match_id", "team_id"]).reset_index(drop=True)

    print("Match Stats created:", match_stats.shape)


    # ============================================================
    # PART 3 — Build Teams master table
    # ============================================================

    teams_home = fixtures[["teams_home_id", "teams_home_name"]].rename(
        columns={"teams_home_id": "team_id", "teams_home_name": "team_name"}
    )
    teams_away = fixtures[["teams_away_id", "teams_away_name"]].rename(
        columns={"teams_away_id": "team_id", "teams_away_name": "team_name"}
    )
    teams = (
        pd.concat([teams_home, teams_away], ignore_index=True)
        .drop_duplicates()
        .sort_values("team_id")
        .reset_index(drop=True)
    )

    duplicate_team_ids = teams["team_id"].duplicated().sum()
    if duplicate_team_ids != 0:
        raise ValueError(f"Teams has {duplicate_team_ids} duplicate team_id values.")

    print("Teams created:", teams.shape)


    # ============================================================
    # PART 4 — Prepare Player Stats
    # ============================================================
    # Grain: one row = one player in one match. PK: match_id + player_id

    player_stats = player_stats.rename(columns={"fixture_id": "match_id"})

    duplicate_player_pk = player_stats.duplicated(["match_id", "player_id"]).sum()
    if duplicate_player_pk != 0:
        raise ValueError(f"Player Stats has {duplicate_player_pk} duplicate (match_id, player_id) keys.")

    player_stats = player_stats.sort_values(["match_id", "player_id"]).reset_index(drop=True)

    print("Player Stats ready:", player_stats.shape)


    # ============================================================
    # PART 4.5 — Cross-source team-name map + match lookup
    # ============================================================
    # Shared by Match Events and Weather below: both need to recover the
    # project's match_id via (date + home team + away team), since neither
    # source uses the API-Football match id.

    source_team_name_map = {
        "Abha": "Abha", "Al Adalah": "Al-Adalah", "Al Ahli": "Al-Ahli Jeddah",
        "Al Batin": "Al Baten", "Al Diriyah": "Al Diriyah", "Al Ettifaq": "Al-Ettifaq",
        "Al Faisaly": "Al-Faisaly FC", "Al Fateh": "Al-Fateh", "Al Fayha": "Al-Fayha",
        "Al Hazem": "Al-Hazm", "Al Hilal": "Al-Hilal Saudi FC", "Al Ittihad": "Al-Ittihad FC",
        "Al Khaleej": "Al Khaleej Saihat", "Al Kholood": "Al Kholood", "Al Najma": "Al Najma",
        "Al Nassr": "Al-Nassr", "Al Okhdood": "Al Okhdood", "Al Orobah": "Al Orubah",
        "Al Qadsiah": "Al-Qadisiyah FC", "Al Raed": "Al-Raed", "Al Riyadh": "Al Riyadh",
        "Al Shabab": "Al Shabab", "Al Taawoun": "Al Taawon", "Al Tai": "Al Taee",
        "Al Wehda": "Al Wehda Club", "Damac": "Damac", "Neom SC": "NEOM", "NEOM": "NEOM","Al Akhdoud": "Al Okhdood",
    }

    match_lookup = (
        match_stats[["match_id", "fixture_date", "teams_home_name", "teams_away_name"]]
        .drop_duplicates("match_id")
        .copy()
    )
    match_lookup["date_key"] = pd.to_datetime(match_lookup["fixture_date"]).dt.date
    match_lookup["home_team_key"] = match_lookup["teams_home_name"]
    match_lookup["away_team_key"] = match_lookup["teams_away_name"]


    # ============================================================
    # PART 5 — Prepare Match Events + ESPN-to-project crosswalk
    # ============================================================
    # ESPN uses its own match_id system, different from the API-Football
    # id used everywhere else. This crosswalk recovers the project's
    # match_id for each ESPN event using:
    #   match date + standardized home team + standardized away team
    # Events that can't be matched are KEPT with match_id/event_team_id
    # null rather than dropped, so no event data is silently discarded.

    team_name_to_id = dict(zip(teams["team_name"], teams["team_id"]))

    match_events["home_team_key"] = match_events["home_team_name"].map(source_team_name_map)
    match_events["away_team_key"] = match_events["away_team_name"].map(source_team_name_map)

    unmapped_event_teams = sorted(
        set(match_events.loc[match_events["home_team_key"].isna(), "home_team_name"].dropna())
        | set(match_events.loc[match_events["away_team_key"].isna(), "away_team_name"].dropna())
    )
    if unmapped_event_teams:
        print(f"WARNING: unmapped team names in Match Events (left unmatched): {unmapped_event_teams}")

    match_events["date_key"] = pd.to_datetime(match_events["match_date"], utc=True, errors="coerce").dt.date

    match_events = match_events.merge(
        match_lookup[["match_id", "date_key", "home_team_key", "away_team_key"]],
        on=["date_key", "home_team_key", "away_team_key"],
        how="left",
        validate="many_to_one",
    )

    matched_events = match_events["match_id"].notna().sum()
    print(f"Match Events matched to a project match_id: {matched_events} / {len(match_events)}")

    # Map the event's own team (ESPN's team id) to the project's team_id.
    project_home_id = match_events["home_team_key"].map(team_name_to_id)
    project_away_id = match_events["away_team_key"].map(team_name_to_id)

    match_events["espn_event_team_id"] = match_events["event_team_id"]

    is_home_event = match_events["espn_event_team_id"] == match_events["home_team_id"]
    is_away_event = match_events["espn_event_team_id"] == match_events["away_team_id"]

    match_events["event_team_id"] = pd.NA
    match_events.loc[is_home_event, "event_team_id"] = project_home_id[is_home_event]
    match_events.loc[is_away_event, "event_team_id"] = project_away_id[is_away_event]
    match_events["event_team_id"] = pd.to_numeric(match_events["event_team_id"], errors="coerce").astype("Int64")

    resolved_team_count = match_events["event_team_id"].notna().sum()
    print(f"Match Events with a resolved project team_id: {resolved_team_count} / {len(match_events)}")

    match_events = match_events.drop(columns=["date_key", "home_team_key", "away_team_key"])

    duplicate_event_ids = match_events["event_id"].duplicated().sum()
    missing_event_ids = match_events["event_id"].isna().sum()
    if duplicate_event_ids != 0 or missing_event_ids != 0:
        raise ValueError(f"Invalid Match Events event_id: {duplicate_event_ids} duplicates, {missing_event_ids} missing.")

    match_events["_sort"] = pd.to_datetime(match_events["match_date"], utc=True, errors="coerce")
    match_events = match_events.sort_values("_sort").drop(columns="_sort").reset_index(drop=True)

    print("Match Events ready:", match_events.shape)


    # ============================================================
    # PART 6 — Prepare Weather
    # ============================================================
    # Weather doesn't carry the same match_id as Match Stats. Recovered
    # via: match_date + standardized home team + standardized away team.
    # Only Weather records that map to an existing match are kept.

    weather["home_team_key"] = weather["home_team"].map(source_team_name_map)
    weather["away_team_key"] = weather["away_team"].map(source_team_name_map)

    unmapped_home = weather.loc[weather["home_team_key"].isna(), "home_team"].unique().tolist()
    unmapped_away = weather.loc[weather["away_team_key"].isna(), "away_team"].unique().tolist()
    if unmapped_home or unmapped_away:
        # Logged rather than raised: new competitions/teams can appear in
        # ESPN's feed over time (King's Cup, AFC CL, friendlies). Those rows
        # simply won't match a fixture and get excluded.
        print(f"WARNING: unmapped home teams in weather: {unmapped_home}")
        print(f"WARNING: unmapped away teams in weather: {unmapped_away}")

    weather["date_key"] = pd.to_datetime(weather["match_date"]).dt.date

    weather_matched = weather.merge(
        match_lookup[["match_id", "date_key", "home_team_key", "away_team_key"]],
        on=["date_key", "home_team_key", "away_team_key"],
        how="left",
    )

    matched_count = weather_matched["match_id"].notna().sum()
    unmatched_count = weather_matched["match_id"].isna().sum()
    print(f"Weather matched: {matched_count}")
    print(f"Weather unmatched: {unmatched_count}")

    weather_final = weather_matched[weather_matched["match_id"].notna()].copy()
    weather_final = weather_final.drop(columns=["date_key", "home_team_key", "away_team_key"])
    weather_final["match_id"] = weather_final["match_id"].astype(int)

    if weather_final["match_id"].duplicated().any():
        dupe_ids = weather_final.loc[weather_final["match_id"].duplicated(keep=False), "match_id"].unique()
        print(f"WARNING: {len(dupe_ids)} match_id(s) matched more than one weather row — keeping the first.")
        weather_final = weather_final.drop_duplicates(subset="match_id", keep="first")

    print("Final Weather:", weather_final.shape)

    if "match_date" in weather_final.columns:
        weather_final["_sort"] = pd.to_datetime(weather_final["match_date"], errors="coerce")
        weather_final = weather_final.sort_values("_sort").drop(columns="_sort").reset_index(drop=True)


    # ============================================================
    # PART 7 — Prepare H2H
    # ============================================================
    # Historical lookup — NOT merged into Match Stats.

    h2h = h2h.rename(columns={"fixture_id": "h2h_match_id"})

    duplicate_h2h_ids = h2h["h2h_match_id"].duplicated().sum()
    missing_h2h_ids = h2h["h2h_match_id"].isna().sum()
    if duplicate_h2h_ids != 0 or missing_h2h_ids != 0:
        raise ValueError(f"Invalid H2H primary key: {duplicate_h2h_ids} duplicates, {missing_h2h_ids} missing.")

    team_ids = set(teams["team_id"])
    h2h_team_ids = set(pd.concat([h2h["teams_home_id"], h2h["teams_away_id"]]).dropna())
    missing_h2h_team_ids = h2h_team_ids - team_ids
    if missing_h2h_team_ids:
        # H2H intentionally covers ALL competitions, so it can reference
        # teams that never appear in the Pro League Fixtures/Teams table.
        print(f"WARNING: H2H references team IDs not in Teams (likely other competitions): {missing_h2h_team_ids}")

    if "fixture_date" in h2h.columns:
        h2h["_sort"] = pd.to_datetime(h2h["fixture_date"], errors="coerce")
        h2h = h2h.sort_values("_sort").drop(columns="_sort").reset_index(drop=True)

    print("H2H ready:", h2h.shape)


    # ============================================================
    # PART 8 — Final relational validation
    # ============================================================
    # These raise on failure — that's what makes the Databricks Job report
    # the run as FAILED and fire the notification.

    pk_checks = {
        "Match Stats (match_id, team_id)": match_stats.duplicated(["match_id", "team_id"]).sum(),
        "Teams (team_id)": teams["team_id"].duplicated().sum(),
        "Player Stats (match_id, player_id)": player_stats.duplicated(["match_id", "player_id"]).sum(),
        "Match Events (event_id)": match_events["event_id"].duplicated().sum(),
        "Weather (match_id)": weather_final["match_id"].duplicated().sum(),
        "H2H (h2h_match_id)": h2h["h2h_match_id"].duplicated().sum(),
    }
    for name, count in pk_checks.items():
        if count != 0:
            raise ValueError(f"{name} failed: {count} duplicate keys.")

    team_ids = set(teams["team_id"])
    match_ids = set(match_stats["match_id"])

    fk_checks = {
        "Match Stats -> Teams": len(set(match_stats["team_id"]) - team_ids),
        "Player Stats -> Teams": len(set(player_stats["team_id"]) - team_ids),
        "Player Stats -> Match Stats": len(set(player_stats["match_id"]) - match_ids),
        "Weather -> Match Stats": len(set(weather_final["match_id"]) - match_ids),
    }
    for name, count in fk_checks.items():
        if count != 0:
            raise ValueError(f"{name} failed: {count} missing references.")

    matched_event_match_ids = set(match_events["match_id"].dropna()) - match_ids
    if matched_event_match_ids:
        raise ValueError(f"Match Events -> Match Stats failed: {len(matched_event_match_ids)} resolved match_id(s) not in Match Stats.")
    resolved_events = match_events["match_id"].notna().sum()
    print(f"Match Events resolved to a Match Stats match_id: {resolved_events} / {len(match_events)}")

    resolved_event_team_ids = set(match_events["event_team_id"].dropna()) - team_ids
    if resolved_event_team_ids:
        raise ValueError(f"Match Events -> Teams failed: {len(resolved_event_team_ids)} resolved event_team_id(s) not in Teams.")
    resolved_event_teams = match_events["event_team_id"].notna().sum()
    print(f"Match Events with event_team_id resolved to Teams: {resolved_event_teams} / {len(match_events)}")

    print("All primary-key and foreign-key validations passed.")


    # ============================================================
    # PART 9 — Filter to NEW rows only, upsert into gold Delta tables
    # ============================================================
    # Everything above ran on the full data (needed for correct lookups).
    # Here each result is cut down to rows not already in gold, then
    # MERGEd in — so a re-run costs nothing and late corrections still
    # overwrite the matching row.

    new_match_ids = set(match_stats["match_id"]) - existing_match_ids
    print(f"\nNew matches this run: {len(new_match_ids)}")

    teams_new = teams[~teams["team_id"].isin(existing_team_ids)].copy()
    match_stats_new = match_stats[match_stats["match_id"].isin(new_match_ids)].copy()
    player_stats_new = player_stats[player_stats["match_id"].isin(new_match_ids)].copy()
    match_events_new = match_events[~match_events["event_id"].isin(existing_event_ids)].copy()
    weather_new = weather_final[weather_final["match_id"].isin(new_match_ids)].copy()
    h2h_new = h2h[~h2h["h2h_match_id"].isin(existing_h2h_ids)].copy()

    upsert_to_gold(teams_new, "teams", merge_keys=["team_id"])
    upsert_to_gold(match_stats_new, "match_stats", merge_keys=["match_id", "team_id"])
    upsert_to_gold(player_stats_new, "player_stats", merge_keys=["match_id", "player_id"])
    upsert_to_gold(match_events_new, "match_events", merge_keys=["event_id"])
    upsert_to_gold(weather_new, "weather", merge_keys=["match_id"])
    upsert_to_gold(h2h_new, "h2h", merge_keys=["h2h_match_id"])

    print("\nGold layer complete (incremental).")