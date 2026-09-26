"""Pipeline stage extracted from 02_profile_clean.ipynb.
Existing notebook logic is preserved inside run_profile_clean().
"""

def run_profile_clean():
    # ============================================================
    # SHARED PROJECT CONFIGURATION
    # ============================================================
    # config.yaml contains non-secret configuration.
    # API/storage credentials remain in Databricks Secrets.

    from pathlib import Path
    import yaml

    def find_project_file(filename="config.yaml"):
        current = Path.cwd()
        for directory in [current, *current.parents]:
            candidate = directory / filename
            if candidate.exists():
                return candidate
        raise FileNotFoundError(
            f"{filename} was not found in the current directory or its parents."
        )

    CONFIG_PATH = find_project_file()
    with CONFIG_PATH.open("r", encoding="utf-8") as f:
        CONFIG = yaml.safe_load(f)

    STORAGE_ACCOUNT = CONFIG["storage"]["account"]
    BRONZE_CONTAINER = CONFIG["storage"]["bronze_container"]
    SILVER_CONTAINER = CONFIG["storage"]["silver_container"]

    SECRET_SCOPE = CONFIG["secrets"]["databricks_scope"]
    STORAGE_KEY_SECRET = CONFIG["secrets"]["storage_account_key"]
    API_FOOTBALL_KEY_SECRET = CONFIG["secrets"]["api_football_key"]

    SPL_LEAGUE_ID = CONFIG["competition"]["league_id"]
    SPL_SEASON = CONFIG["competition"]["season"]
    TIMEZONE = CONFIG["competition"]["timezone"]

    API_FOOTBALL_BASE_URL = CONFIG["apis"]["api_football"]["base_url"]
    FIXTURES_URL = API_FOOTBALL_BASE_URL + CONFIG["apis"]["api_football"]["fixtures_endpoint"]
    FIXTURE_STATS_URL = API_FOOTBALL_BASE_URL + CONFIG["apis"]["api_football"]["fixture_stats_endpoint"]
    FIXTURE_PLAYERS_URL = API_FOOTBALL_BASE_URL + CONFIG["apis"]["api_football"]["fixture_players_endpoint"]
    H2H_URL = API_FOOTBALL_BASE_URL + CONFIG["apis"]["api_football"]["h2h_endpoint"]

    ESPN_SCOREBOARD_URL = CONFIG["apis"]["espn"]["scoreboard_url"]
    OPEN_METEO_URL = CONFIG["apis"]["open_meteo"]["historical_url"]

    REQUEST_TIMEOUT = CONFIG["request"]["timeout_seconds"]
    MAX_RETRIES = CONFIG["request"]["max_retries"]
    TIMEOUT_RETRY_SLEEP = CONFIG["request"]["timeout_retry_sleep_seconds"]
    RATE_LIMIT_SLEEP = CONFIG["request"]["rate_limit_sleep_seconds"]
    BETWEEN_REQUESTS_SLEEP = CONFIG["request"]["between_requests_seconds"]

    FULL_REBUILD = CONFIG["weather"]["full_rebuild"]
    START_DATE = CONFIG["weather"]["start_date"]

    def get_secret(secret_name):
        return dbutils.secrets.get(scope=SECRET_SCOPE, key=secret_name)

    STORAGE_KEY = get_secret(STORAGE_KEY_SECRET)
    API_FOOTBALL_KEY = get_secret(API_FOOTBALL_KEY_SECRET)

    print(f"Loaded configuration from: {CONFIG_PATH}")
    print(f"Storage account: {STORAGE_ACCOUNT}")
    print(f"League ID: {SPL_LEAGUE_ID}")
    print(f"Season: {SPL_SEASON}")


    # ============================================================
    # CELL  — silver transformation (INCREMENTAL)
    # ============================================================
    # Databricks notebook: silver_fixtures_stats  (incremental)
    # Same cleaning as before, but only processes bronze fixtures/stats
    # from date folders newer than the watermark, then merges into the
    # existing silver files.
    #
    # Watermark: newest fixture_date already in silver/fixtures. On the
    # first run (no silver yet) there's no watermark, so ALL bronze is
    # read — including the one-off historical/ backfill folder — which is
    # a normal full build. After that, only new daily folders are read.
    #
    # Two outputs kept in sync:
    #   silver/fixtures/cleaned_fixtures.csv     (dedup by fixture_id)
    #   silver/match_stats/cleaned_stats.csv     (dedup by fixture_id+team)
    #
    # Input:  bronze/fixtures/**/*.json, bronze/match_stats/**/*.json
    # Output: silver/fixtures/cleaned_fixtures.csv
    #         silver/match_stats/cleaned_stats.csv

    import re
    import io
    import json
    import numpy as np
    import pandas as pd
    from azure.storage.filedatalake import DataLakeServiceClient

    # --- Storage access (Azure SDK, not Spark) ---
    storage_account = STORAGE_ACCOUNT

    storage_key = STORAGE_KEY

    service_client = DataLakeServiceClient(
        account_url=f"https://{storage_account}.dfs.core.windows.net",
        credential=storage_key,
    )

    bronze_fs = service_client.get_file_system_client(file_system=BRONZE_CONTAINER)
    silver_fs = service_client.get_file_system_client(file_system=SILVER_CONTAINER)


    def to_snake_case(col: str) -> str:
        col = col.replace(".", "_").replace(" ", "_")
        return col.lower()


    def read_existing_csv(fs_client, path: str):
        try:
            file_client = fs_client.get_file_client(path)
            content = file_client.download_file().readall()
            return pd.read_csv(io.BytesIO(content))
        except Exception:
            return None


    def save_csv_to_silver(df: pd.DataFrame, dir_name: str, file_name: str) -> None:
        silver_dir = silver_fs.get_directory_client(dir_name)
        silver_dir.create_directory()
        csv_bytes = df.to_csv(index=False).encode("utf-8")
        file_client = silver_dir.create_file(file_name)
        file_client.upload_data(csv_bytes, overwrite=True)


    def list_new_json_files(top_path: str, watermark) -> list:
        """Return .json files under bronze/<top_path>/ whose date folder is
        >= watermark. On first run (watermark None) returns everything,
        including the historical/ backfill folder."""
        try:
            entries = [p for p in bronze_fs.get_paths(path=top_path, recursive=False)]
        except Exception:
            print(f"NOTE: bronze/{top_path}/ doesn't exist yet — treating as no data.")
            return []

        wanted_files = []
        for entry in entries:
            name = entry.name.split("/")[-1]

            # First-run: take everything (daily folders + historical).
            if watermark is None:
                if entry.is_directory:
                    wanted_files += [
                        p.name for p in bronze_fs.get_paths(path=entry.name, recursive=True)
                        if not p.is_directory and p.name.endswith(".json")
                    ]
                elif entry.name.endswith(".json"):
                    wanted_files.append(entry.name)
                continue

            # Incremental: only date folders >= watermark. The historical/
            # folder isn't a date, so it's skipped after the first build
            # (already merged into silver).
            if entry.is_directory:
                try:
                    folder_date = pd.to_datetime(name).date()
                except Exception:
                    continue
                if folder_date >= watermark:
                    wanted_files += [
                        p.name for p in bronze_fs.get_paths(path=entry.name, recursive=True)
                        if not p.is_directory and p.name.endswith(".json")
                    ]
        return wanted_files


    def load_fixture_objects(paths: list) -> list:
        objs = []
        for path in paths:
            content = bronze_fs.get_file_client(path).download_file().readall()
            data = json.loads(content)
            if isinstance(data, dict):
                objs.extend(data.get("response", []))
            elif isinstance(data, list):
                objs.extend(data)
        return objs


    # ============================================================
    # PART 0 — watermark from existing silver fixtures
    # ============================================================

    existing_fixtures = read_existing_csv(silver_fs, "fixtures/cleaned_fixtures.csv")
    existing_stats = read_existing_csv(silver_fs, "match_stats/cleaned_stats.csv")

    if existing_fixtures is not None and "fixture_date" in existing_fixtures.columns and not existing_fixtures.empty:
        watermark = pd.to_datetime(existing_fixtures["fixture_date"]).max().date()
        print(f"Watermark (newest fixture_date in silver): {watermark}")
    else:
        watermark = None
        print("No watermark — full build (reads historical + all daily folders).")


    # ============================================================
    # PART 1 — Fixtures (new only)
    # ============================================================

    fixture_paths = list_new_json_files("fixtures", watermark)
    print(f"Fixture raw files to process this run: {len(fixture_paths)}")

    new_fixture_objs = load_fixture_objects(fixture_paths)

    df_new_fixtures = None
    if new_fixture_objs:
        df_new_fixtures = pd.json_normalize(new_fixture_objs).drop_duplicates(subset="fixture.id")

        cols_to_drop = [
            "score.extratime.home", "score.extratime.away",
            "score.penalty.home", "score.penalty.away",
        ] + [c for c in df_new_fixtures.columns if "logo" in c or "flag" in c]
        df_new_fixtures = df_new_fixtures.drop(columns=cols_to_drop, errors="ignore")

        df_new_fixtures["fixture.date"] = pd.to_datetime(df_new_fixtures["fixture.date"])
        df_new_fixtures.columns = [to_snake_case(c) for c in df_new_fixtures.columns]

    # We need the full fixtures set (old + new) to build placeholder stats
    # rows for any new fixture that has no stats block.
    fixtures_existing_count = 0 if existing_fixtures is None else len(existing_fixtures)
    fixtures_frames = [f for f in (existing_fixtures, df_new_fixtures) if f is not None and not f.empty]
    if not fixtures_frames:
        dbutils.notebook.exit("No fixtures data at all — nothing to write.")
    fixtures_all = pd.concat(fixtures_frames, ignore_index=True)
    fixtures_all = fixtures_all.drop_duplicates(subset="fixture_id", keep="last").reset_index(drop=True)
    # Sort fixtures chronologically (oldest first).
    fixtures_all["fixture_date"] = pd.to_datetime(fixtures_all["fixture_date"], errors="coerce")
    fixtures_all = fixtures_all.sort_values("fixture_date").reset_index(drop=True)


    # ============================================================
    # PART 2 — Match stats (new only)
    # ============================================================

    stats_paths = list_new_json_files("match_stats", watermark)
    print(f"Match-stats raw files to process this run: {len(stats_paths)}")

    flat_rows = []
    for path in stats_paths:
        content = bronze_fs.get_file_client(path).download_file().readall()
        data = json.loads(content)

        if isinstance(data, dict) and "response" in data:
            match = re.search(r"fixture_(\d+)_statistics\.json$", path)
            if not match:
                continue
            fixture_id = int(match.group(1))
            for team_block in data.get("response", []):
                row = {"fixture_id": fixture_id, "team": team_block.get("team", {}).get("name")}
                for stat in team_block.get("statistics", []):
                    row[stat["type"]] = stat["value"]
                flat_rows.append(row)
        elif isinstance(data, list):
            for entry in data:
                fixture_id = entry.get("fixture_id")
                for team_block in entry.get("statistics", []):
                    row = {"fixture_id": fixture_id, "team": team_block.get("team", {}).get("name")}
                    for stat in team_block.get("statistics", []):
                        row[stat["type"]] = stat["value"]
                    flat_rows.append(row)

    df_new_stats = pd.DataFrame(flat_rows) if flat_rows else None

    if df_new_stats is not None and not df_new_stats.empty:
        # Placeholder rows: any NEW fixture with no stats still gets its two
        # team rows (NaN), so every fixture keeps both rows for the join.
        stat_columns = [c for c in df_new_stats.columns if c not in ("fixture_id", "team")]
        new_fixture_ids = set(df_new_fixtures["fixture_id"]) if df_new_fixtures is not None else set()
        have_stats = set(df_new_stats["fixture_id"])
        missing = new_fixture_ids - have_stats
        if missing:
            placeholder_rows = []
            for fid in missing:
                row_match = fixtures_all[fixtures_all["fixture_id"] == fid].iloc[0]
                for team_name in (row_match["teams_home_name"], row_match["teams_away_name"]):
                    r = {"fixture_id": fid, "team": team_name}
                    for col in stat_columns:
                        r[col] = np.nan
                    placeholder_rows.append(r)
            df_new_stats = pd.concat([df_new_stats, pd.DataFrame(placeholder_rows)], ignore_index=True)

        df_new_stats = df_new_stats.drop(columns=["Free Kicks", "goals_prevented"], errors="ignore")
        for col in ["Red Cards", "Yellow Cards", "Offsides"]:
            if col in df_new_stats.columns:
                df_new_stats[col] = df_new_stats[col].fillna(0)
        if "Ball Possession" in df_new_stats.columns:
            df_new_stats["Ball Possession"] = (
                df_new_stats["Ball Possession"].astype(str).str.rstrip("%").replace("nan", None).astype(float)
            )
        df_new_stats = df_new_stats.drop_duplicates(subset=["fixture_id", "team"])
        df_new_stats.columns = [to_snake_case(c) for c in df_new_stats.columns]
        if "passes_%" in df_new_stats.columns:
            df_new_stats["passes_%"] = pd.to_numeric(
                df_new_stats["passes_%"].astype(str).str.replace("%", "", regex=False).str.strip(),
                errors="coerce",
            )
        # Same "Dhamk" -> "Damac" fix as before
        df_new_stats["team"] = df_new_stats["team"].replace({"Dhamk": "Damac"})


    # ============================================================
    # PART 3 — Merge stats (existing + new), dedup (new wins)
    # ============================================================

    stats_existing_count = 0 if existing_stats is None else len(existing_stats)
    stats_frames = [f for f in (existing_stats, df_new_stats) if f is not None and not f.empty]
    if stats_frames:
        stats_all = pd.concat(stats_frames, ignore_index=True)
        stats_all = stats_all.drop_duplicates(subset=["fixture_id", "team"], keep="last").reset_index(drop=True)
        # Stats has no date column; sort by fixture_id (then team) so rows
        # for the same match sit together and newer fixtures fall at the end.
        stats_all = stats_all.sort_values(["fixture_id", "team"]).reset_index(drop=True)
    else:
        stats_all = existing_stats if existing_stats is not None else pd.DataFrame()


    # ============================================================
    # PART 4 — Save + summary
    # ============================================================

    save_csv_to_silver(fixtures_all, "fixtures", "cleaned_fixtures.csv")
    save_csv_to_silver(stats_all, "match_stats", "cleaned_stats.csv")

    fixtures_added = len(fixtures_all) - fixtures_existing_count
    stats_added = len(stats_all) - stats_existing_count

    print("\n============= SILVER FIXTURES + STATS SUMMARY =============")
    print(f"Fixtures: {fixtures_existing_count} existing + {fixtures_added if fixtures_added>0 else 0} new = {len(fixtures_all)} total")
    print(f"Stats   : {stats_existing_count} existing + {stats_added if stats_added>0 else 0} new = {len(stats_all)} total")
    print("Saved silver/fixtures/cleaned_fixtures.csv")
    print("Saved silver/match_stats/cleaned_stats.csv")
    print("==========================================================")

    # ============================================================
    # silver_player_stats  (INCREMENTAL)
    # ============================================================
    # Flattens raw player-stats files into one row per player per fixture.
    #
    # Incremental by design: player-stats rows carry no date of their own,
    # so the watermark is the set of fixture_ids already in silver. Every
    # run lists the raw file names (a cheap metadata call), then downloads
    # and parses only the files for fixtures not yet loaded. The merge at
    # the end keeps existing rows and appends the new ones.
    #
    # First run (no silver yet) => every file is new => full build.
    #
    # Input:  bronze/match_player_stats/**/*.json  (fixture_<id>_players.json)
    # Output: silver/player_stats/cleaned_player_stats.csv

    import re
    import io
    import json
    import pandas as pd
    from azure.storage.filedatalake import DataLakeServiceClient

    # --- Storage access (Azure SDK, not Spark) ---
    storage_account = STORAGE_ACCOUNT

    storage_key = STORAGE_KEY

    service_client = DataLakeServiceClient(
        account_url=f"https://{storage_account}.dfs.core.windows.net",
        credential=storage_key,
    )

    bronze_fs = service_client.get_file_system_client(file_system=BRONZE_CONTAINER)
    silver_fs = service_client.get_file_system_client(file_system=SILVER_CONTAINER)

    BRONZE_TOP = "match_player_stats"
    SILVER_DIR = "player_stats"
    SILVER_FILE = "cleaned_player_stats.csv"


    def read_json(fs_client, path: str) -> dict:
        content = fs_client.get_file_client(path).download_file().readall()
        return json.loads(content)


    def read_existing_silver():
        """Return (df, set of fixture_ids already loaded), or (None, set())
        when there is no silver file yet."""
        try:
            content = silver_fs.get_file_client(f"{SILVER_DIR}/{SILVER_FILE}").download_file().readall()
            df = pd.read_csv(io.BytesIO(content))
            if df.empty or "fixture_id" not in df.columns:
                return (df if not df.empty else None), set()
            return df, set(df["fixture_id"].dropna().astype(int))
        except Exception:
            print("No existing silver/player_stats file — this will be a full build.")
            return None, set()


    def list_new_json_files(done_fixture_ids: set) -> list:
        """Raw files whose fixture_id isn't in silver yet.

        The fixture id lives in the file name, so a file can be skipped
        without downloading it — listing names is a metadata call, parsing
        JSON is the expensive part."""
        try:
            paths = [
                p.name for p in bronze_fs.get_paths(path=BRONZE_TOP, recursive=True)
                if not p.is_directory and p.name.endswith(".json")
            ]
        except Exception:
            print(f"NOTE: bronze/{BRONZE_TOP}/ doesn't exist yet — no data.")
            return []

        wanted = []
        for path in paths:
            match = re.search(r"fixture_(\d+)_players\.json$", path)
            if not match:
                print(f"Skipping unexpected file name: {path}")
                continue
            if int(match.group(1)) in done_fixture_ids:
                continue
            wanted.append(path)
        return wanted


    # ============================================================
    # PART 0 — existing silver + the fixture_ids it already covers
    # ============================================================

    existing_df, done_fixture_ids = read_existing_silver()
    if existing_df is None:
        print("Full build — no silver file yet.")
    else:
        print(f"Incremental — silver has {len(existing_df)} rows covering {len(done_fixture_ids)} fixtures.")


    # ============================================================
    # PART 1 — Flatten NEW raw files only
    # ============================================================

    player_stats_paths = list_new_json_files(done_fixture_ids)
    print(f"Player-stats raw files to process this run: {len(player_stats_paths)}")

    rows = []
    for path in player_stats_paths:
        match = re.search(r"fixture_(\d+)_players\.json$", path)
        fixture_id = int(match.group(1))

        data = read_json(bronze_fs, path)
        for fixture_entry in data.get("response", []):
            team_id = fixture_entry.get("team", {}).get("id")
            for player_entry in fixture_entry.get("players", []):
                player = player_entry.get("player", {})
                stats_list = player_entry.get("statistics", [])
                if not stats_list:
                    continue
                stat = stats_list[0]
                rows.append({
                    "fixture_id": fixture_id,
                    "team_id": team_id,
                    "player_id": player.get("id"),
                    "player_name": player.get("name"),
                    "games_minutes": stat.get("games", {}).get("minutes"),
                    "games_position": stat.get("games", {}).get("position"),
                    "games_rating": stat.get("games", {}).get("rating"),
                    "games_captain": stat.get("games", {}).get("captain"),
                    "games_substitute": stat.get("games", {}).get("substitute"),
                    "goals_total": stat.get("goals", {}).get("total"),
                    "goals_assists": stat.get("goals", {}).get("assists"),
                    "goals_saves": stat.get("goals", {}).get("saves"),
                    "shots_total": stat.get("shots", {}).get("total"),
                    "shots_on": stat.get("shots", {}).get("on"),
                    "passes_total": stat.get("passes", {}).get("total"),
                    "passes_key": stat.get("passes", {}).get("key"),
                    "passes_accuracy": stat.get("passes", {}).get("accuracy"),
                    "tackles_total": stat.get("tackles", {}).get("total"),
                    "tackles_blocks": stat.get("tackles", {}).get("blocks"),
                    "tackles_interceptions": stat.get("tackles", {}).get("interceptions"),
                    "duels_total": stat.get("duels", {}).get("total"),
                    "duels_won": stat.get("duels", {}).get("won"),
                    "dribbles_attempts": stat.get("dribbles", {}).get("attempts"),
                    "dribbles_success": stat.get("dribbles", {}).get("success"),
                    "fouls_drawn": stat.get("fouls", {}).get("drawn"),
                    "fouls_committed": stat.get("fouls", {}).get("committed"),
                    "cards_yellow": stat.get("cards", {}).get("yellow"),
                    "cards_red": stat.get("cards", {}).get("red"),
                    "penalty_won": stat.get("penalty", {}).get("won"),
                    "penalty_scored": stat.get("penalty", {}).get("scored"),
                    "penalty_missed": stat.get("penalty", {}).get("missed"),
                    "penalty_saved": stat.get("penalty", {}).get("saved"),
                })

    df_new = pd.DataFrame(rows) if rows else None


    # ============================================================
    # PART 2 — Clean the new rows
    # ============================================================

    if df_new is not None and not df_new.empty:
        for col in ["fixture_id", "team_id", "player_id", "games_minutes"]:
            df_new[col] = df_new[col].astype("Int64")
        df_new["games_rating"] = pd.to_numeric(df_new["games_rating"], errors="coerce")
        df_new["passes_accuracy"] = pd.to_numeric(df_new["passes_accuracy"], errors="coerce")
        df_new = df_new.drop_duplicates(subset=["fixture_id", "player_id"])
    else:
        print("No new player-stats rows found.")


    # ============================================================
    # PART 3 — Merge existing + new, dedup (new wins), sort
    # ============================================================

    existing_count = 0 if existing_df is None else len(existing_df)
    frames = [f for f in (existing_df, df_new) if f is not None and not f.empty]
    if not frames:
        dbutils.notebook.exit("No player-stats data at all — nothing to write.")

    combined = pd.concat(frames, ignore_index=True)
    combined = combined.drop_duplicates(subset=["fixture_id", "player_id"], keep="last").reset_index(drop=True)

    # Sort by fixture_id (then player_id) so rows for the same match sit
    # together and newer fixtures (higher ids) fall at the bottom.
    combined = combined.sort_values(["fixture_id", "player_id"]).reset_index(drop=True)

    new_added = len(combined) - existing_count


    # ============================================================
    # PART 4 — Save + summary
    # ============================================================

    silver_dir = silver_fs.get_directory_client(SILVER_DIR)
    silver_dir.create_directory()
    csv_bytes = combined.to_csv(index=False).encode("utf-8")
    silver_dir.create_file(SILVER_FILE).upload_data(csv_bytes, overwrite=True)

    print("\n================= SILVER PLAYER STATS SUMMARY =================")
    print(f"Rows already in silver : {existing_count}")
    print(f"New/updated rows merged: {new_added if new_added > 0 else 0}")
    print(f"Total rows now         : {len(combined)}")
    print(f"Saved silver/{SILVER_DIR}/{SILVER_FILE}")
    print("==============================================================")

    # ============================================================
    # CELL  — silver transformation (INCREMENTAL)
    # ============================================================
    # Databricks notebook: silver_match_events  (incremental)
    # Same cleaning as before, but only processes bronze match_events from
    # date folders newer than the watermark, then merges into the existing
    # silver file. Output is sorted by match_date (oldest first).
    #
    # Watermark: newest match_date already in silver/match_events. First
    # run (no silver) => no watermark => reads ALL bronze incl. historical/
    # (a normal full build). After that, only new daily folders.
    #
    # Input:  bronze/match_events/**/*.json  (ESPN scoreboard format)
    # Output: silver/match_events/cleaned_match_events.csv

    import io
    import json
    import pandas as pd
    from azure.storage.filedatalake import DataLakeServiceClient

    # --- Storage access (Azure SDK, not Spark) ---
    storage_account = STORAGE_ACCOUNT

    storage_key = STORAGE_KEY

    service_client = DataLakeServiceClient(
        account_url=f"https://{storage_account}.dfs.core.windows.net",
        credential=storage_key,
    )

    bronze_fs = service_client.get_file_system_client(file_system=BRONZE_CONTAINER)
    silver_fs = service_client.get_file_system_client(file_system=SILVER_CONTAINER)

    SILVER_DIR = "match_events"
    SILVER_FILE = "cleaned_match_events.csv"


    def read_json(fs_client, path: str) -> dict:
        content = fs_client.get_file_client(path).download_file().readall()
        return json.loads(content)


    def get_season(match_date: pd.Timestamp) -> str:
        if match_date.month >= 8:
            return f"{match_date.year}-{str(match_date.year + 1)[-2:]}"
        return f"{match_date.year - 1}-{str(match_date.year)[-2:]}"


    def read_existing_silver():
        """Return (dataframe, watermark_date) or (None, None) for a full build."""
        try:
            content = silver_fs.get_file_client(f"{SILVER_DIR}/{SILVER_FILE}").download_file().readall()
            df = pd.read_csv(io.BytesIO(content))
            if df.empty or "match_date" not in df.columns:
                return df, None
            watermark = pd.to_datetime(df["match_date"], utc=True, errors="coerce").max()
            return df, (watermark.date() if pd.notna(watermark) else None)
        except Exception:
            print("No existing silver/match_events file — this will be a full build.")
            return None, None


    def list_new_json_files(watermark) -> list:
        """.json files under bronze/match_events/ from date folders >= watermark.
        First run (watermark None) => everything, incl. historical/."""
        try:
            entries = [p for p in bronze_fs.get_paths(path="match_events", recursive=False)]
        except Exception:
            print("NOTE: bronze/match_events/ doesn't exist yet — no data.")
            return []

        wanted = []
        for entry in entries:
            name = entry.name.split("/")[-1]
            if watermark is None:
                if entry.is_directory:
                    wanted += [
                        p.name for p in bronze_fs.get_paths(path=entry.name, recursive=True)
                        if not p.is_directory and p.name.endswith(".json")
                    ]
                elif entry.name.endswith(".json"):
                    wanted.append(entry.name)
                continue
            if entry.is_directory:
                try:
                    folder_date = pd.to_datetime(name).date()
                except Exception:
                    continue
                if folder_date >= watermark:
                    wanted += [
                        p.name for p in bronze_fs.get_paths(path=entry.name, recursive=True)
                        if not p.is_directory and p.name.endswith(".json")
                    ]
        return wanted


    # ============================================================
    # PART 0 — existing silver + watermark
    # ============================================================

    existing_df, watermark = read_existing_silver()
    if watermark is not None:
        print(f"Watermark (newest match_date in silver): {watermark}")
    else:
        print("No watermark — full build (reads historical + all daily folders).")


    # ============================================================
    # PART 1 — Extract event rows from NEW raw files only
    # ============================================================

    event_paths = list_new_json_files(watermark)
    print(f"Match-events raw files to process this run: {len(event_paths)}")

    event_rows = []
    for path in event_paths:
        try:
            data = read_json(bronze_fs, path)
        except Exception as e:
            print(f"Could not read {path}: {e}")
            continue

        for match in data.get("events", []):
            competitions = match.get("competitions", [])
            if not competitions:
                continue
            competition = competitions[0]

            match_id = match.get("id")
            match_date = match.get("date")

            home_team_id = home_team_name = away_team_id = away_team_name = None
            for competitor in competition.get("competitors", []):
                team = competitor.get("team", {})
                home_away = competitor.get("homeAway")
                if home_away == "home":
                    home_team_id, home_team_name = team.get("id"), team.get("displayName")
                elif home_away == "away":
                    away_team_id, away_team_name = team.get("id"), team.get("displayName")

            season = None
            parsed_date = pd.to_datetime(match_date, utc=True, errors="coerce")
            if pd.notna(parsed_date):
                season = get_season(parsed_date)

            details = competition.get("details", [])
            for event_number, event in enumerate(details, start=1):
                event_type = event.get("type", {})
                clock = event.get("clock", {})
                team = event.get("team", {})
                athletes = event.get("athletesInvolved", []) or [None]

                for athlete in athletes:
                    event_rows.append({
                        "event_id": f"{match_id}_{event_number}",
                        "espn_match_id": match_id,
                        "season": season,
                        "match_date": match_date,
                        "home_team_id": home_team_id,
                        "home_team_name": home_team_name,
                        "away_team_id": away_team_id,
                        "away_team_name": away_team_name,
                        "event_type_id": event_type.get("id"),
                        "event_type": event_type.get("text"),
                        "event_seconds": clock.get("value"),
                        "event_minute": clock.get("displayValue"),
                        "event_team_id": team.get("id"),
                        "player_id": athlete.get("id") if athlete else None,
                        "player_name": athlete.get("displayName") if athlete else None,
                        "player_position": athlete.get("position") if athlete else None,
                        "score_value": event.get("scoreValue"),
                        "scoring_play": event.get("scoringPlay"),
                        "yellow_card": event.get("yellowCard"),
                        "red_card": event.get("redCard"),
                        "penalty_kick": event.get("penaltyKick"),
                        "own_goal": event.get("ownGoal"),
                        "shootout": event.get("shootout"),
                    })

    df_new = pd.DataFrame(event_rows) if event_rows else None


    # ============================================================
    # PART 2 — Clean the new rows (same steps as before)
    # ============================================================

    if df_new is not None and not df_new.empty:
        df_new.columns = df_new.columns.str.strip().str.lower().str.replace(" ", "_")

        text_columns = [
            "season", "home_team_name", "away_team_name",
            "event_type", "event_minute", "player_name", "player_position",
        ]
        for col in text_columns:
            df_new[col] = df_new[col].astype("string").str.strip()
            df_new[col] = df_new[col].replace("", pd.NA)

        df_new["match_date"] = pd.to_datetime(df_new["match_date"], errors="coerce", utc=True)

        id_columns = [
            "event_id", "espn_match_id", "home_team_id", "away_team_id",
            "event_type_id", "event_team_id", "player_id",
        ]
        for col in id_columns:
            df_new[col] = df_new[col].astype("string")

        df_new["event_seconds"] = pd.to_numeric(df_new["event_seconds"], errors="coerce")
        df_new["score_value"] = pd.to_numeric(df_new["score_value"], errors="coerce")

        boolean_columns = [
            "scoring_play", "yellow_card", "red_card",
            "penalty_kick", "own_goal", "shootout",
        ]
        for col in boolean_columns:
            df_new[col] = df_new[col].astype("boolean")

        df_new = df_new.drop_duplicates(subset=["event_id"], keep="first")
    else:
        print("No new match-events found in the processed folders.")


    # ============================================================
    # PART 3 — Merge existing + new, dedup (new wins), sort
    # ============================================================

    existing_count = 0 if existing_df is None else len(existing_df)
    frames = [f for f in (existing_df, df_new) if f is not None and not f.empty]
    if not frames:
        dbutils.notebook.exit("No match-events data at all — nothing to write.")

    combined = pd.concat(frames, ignore_index=True)
    combined = combined.drop_duplicates(subset=["event_id"], keep="last").reset_index(drop=True)

    # Sort chronologically (oldest first) so the file reads in time order.
    combined["_sort"] = pd.to_datetime(combined["match_date"], utc=True, errors="coerce")
    combined = combined.sort_values("_sort").drop(columns="_sort").reset_index(drop=True)

    new_added = len(combined) - existing_count


    # ============================================================
    # PART 4 — Save + summary
    # ============================================================

    silver_dir = silver_fs.get_directory_client(SILVER_DIR)
    silver_dir.create_directory()
    csv_bytes = combined.to_csv(index=False).encode("utf-8")
    silver_dir.create_file(SILVER_FILE).upload_data(csv_bytes, overwrite=True)

    print("\n================= SILVER MATCH EVENTS SUMMARY =================")
    print(f"Rows already in silver : {existing_count}")
    print(f"New/updated rows merged: {new_added if new_added > 0 else 0}")
    print(f"Total rows now         : {len(combined)}")
    print(f"Saved silver/{SILVER_DIR}/{SILVER_FILE}")
    print("==============================================================")

    # ============================================================
    # CELL  — silver transformation (INCREMENTAL)
    # ============================================================
    # Databricks notebook: silver_h2h  (incremental)
    # Purpose: same cleaning as before, but only process bronze H2H date
    # folders NEWER than what's already in silver, then merge into the
    # existing silver file (dedup by fixture_id, newest wins).
    #
    # How "incremental" works here:
    #   1. Read the existing silver file (if any). Its newest fixture_date
    #      is the WATERMARK — everything up to it is already processed.
    #   2. List bronze/h2h/<date>/ folders and keep only those whose date
    #      is >= the watermark date (>= not >, so a match added later on an
    #      already-seen day isn't missed; dedup handles the overlap).
    #   3. Clean only those new folders.
    #   4. Concat old silver + new, drop_duplicates on fixture_id keeping
    #      the NEW row (last), and save.
    #   5. Print a summary (existing / new / total).
    #
    # First run (no silver yet) => no watermark => reads ALL bronze (a
    # normal full build). Every run after that is incremental.
    #
    # Input:  bronze/h2h/<date>/*.json
    # Output: silver/h2h/cleaned_h2h.csv

    import re
    import io
    import json
    import pandas as pd
    from azure.storage.filedatalake import DataLakeServiceClient

    # --- Storage access (Azure SDK, not Spark) ---
    storage_account = STORAGE_ACCOUNT

    storage_key = STORAGE_KEY

    service_client = DataLakeServiceClient(
        account_url=f"https://{storage_account}.dfs.core.windows.net",
        credential=storage_key,
    )

    bronze_fs = service_client.get_file_system_client(file_system=BRONZE_CONTAINER)
    silver_fs = service_client.get_file_system_client(file_system=SILVER_CONTAINER)

    SILVER_DIR = "h2h"
    SILVER_FILE = "cleaned_h2h.csv"


    def to_snake_case(col: str) -> str:
        col = col.replace(".", "_")
        col = re.sub(r"(?<!^)(?=[A-Z])", "_", col).lower()
        return col


    def read_existing_silver():
        """Return (dataframe, watermark_date). If no silver file yet,
        returns (None, None) so the run falls back to a full build."""
        try:
            file_client = silver_fs.get_file_client(f"{SILVER_DIR}/{SILVER_FILE}")
            content = file_client.download_file().readall()
            df = pd.read_csv(io.BytesIO(content))
            if df.empty or "fixture_date" not in df.columns:
                return df, None

            # H2H covers every competition AND includes fixtures that are
            # scheduled but not yet played, so max(fixture_date) can sit
            # months in the future. Bronze folders are named by the day the
            # extractor ran, so a future watermark filters out every folder
            # forever. Use the newest fixture that has actually been played.
            dates = pd.to_datetime(df["fixture_date"], errors="coerce", utc=True)
            played = dates[dates <= pd.Timestamp.now(tz="UTC")]
            watermark = played.max().date() if played.notna().any() else None
            return df, watermark
        except Exception:
            print("No existing silver/h2h file — this will be a full build.")
            return None, None


    # --- Step 1: existing silver + watermark ---
    existing_df, watermark = read_existing_silver()
    if watermark is not None:
        print(f"Watermark (newest fixture_date already in silver): {watermark}")
    else:
        print("No watermark — processing all bronze H2H folders.")

    # --- Step 2: list bronze date folders, keep only new ones ---
    all_dir_paths = [
        p.name for p in bronze_fs.get_paths(path="h2h", recursive=False)
        if p.is_directory
    ]

    date_folders = []
    for path in all_dir_paths:
        folder_name = path.split("/")[-1]  # e.g. "2026-09-21"
        try:
            folder_date = pd.to_datetime(folder_name).date()
        except Exception:
            continue  # skip anything that isn't a date folder
        if watermark is None or folder_date >= watermark:
            date_folders.append((folder_date, path))

    print(f"Bronze H2H folders to process this run: {len(date_folders)}")

    # --- Step 3: read + clean the new folders ---
    new_fixtures = []
    for _, folder_path in date_folders:
        files = [
            p.name for p in bronze_fs.get_paths(path=folder_path, recursive=True)
            if not p.is_directory and p.name.endswith(".json")
        ]
        for file_path in files:
            file_client = bronze_fs.get_file_client(file_path)
            content = file_client.download_file().readall()
            data = json.loads(content)
            new_fixtures.extend(data.get("response", []))

    if new_fixtures:
        df_new = pd.json_normalize(new_fixtures)
        df_new = df_new.drop_duplicates(subset="fixture.id")

        df_new["fixture.date"] = pd.to_datetime(df_new["fixture.date"])

        cols_to_drop = [c for c in df_new.columns if "logo" in c or "flag" in c]
        cols_to_drop += [c for c in df_new.columns if "extratime" in c or "penalty" in c]
        df_new = df_new.drop(columns=cols_to_drop)

        df_new.columns = [to_snake_case(c) for c in df_new.columns]
    else:
        df_new = None
        print("No new H2H fixtures found in the processed folders.")

    # --- Step 4: merge existing + new, dedup (new wins) ---
    existing_count = 0 if existing_df is None else len(existing_df)

    frames = [f for f in (existing_df, df_new) if f is not None and not f.empty]
    if not frames:
        dbutils.notebook.exit("No H2H data at all (no silver, no bronze) — nothing to write.")

    combined = pd.concat(frames, ignore_index=True)
    # keep="last" => when a fixture_id appears in both old and new, the NEW
    # row (appended last) is the one kept.
    combined = combined.drop_duplicates(subset="fixture_id", keep="last").reset_index(drop=True)

    # Sort chronologically (oldest first) so the file reads in time order.
    combined["fixture_date"] = pd.to_datetime(combined["fixture_date"], errors="coerce")
    combined = combined.sort_values("fixture_date").reset_index(drop=True)

    new_added = len(combined) - existing_count

    # --- Step 5: save + summary ---
    silver_dir = silver_fs.get_directory_client(SILVER_DIR)
    silver_dir.create_directory()
    csv_bytes = combined.to_csv(index=False).encode("utf-8")
    file_client = silver_dir.create_file(SILVER_FILE)
    file_client.upload_data(csv_bytes, overwrite=True)

    print("\n==================== SILVER H2H SUMMARY ====================")
    print(f"Rows already in silver : {existing_count}")
    print(f"New/updated rows merged: {new_added if new_added >= 0 else 0}")
    print(f"Total rows now         : {len(combined)}")
    print(f"Saved silver/{SILVER_DIR}/{SILVER_FILE}")
    print("===========================================================")

    # ============================================================
    # CELL  — silver transformation (INCREMENTAL)
    # ============================================================
    # Databricks notebook: silver_weather  (incremental)
    # The bronze source is a single JSON file that already holds every
    # match+weather record. This notebook cleans ONLY the records whose
    # event_id isn't already in silver, then merges them in. Output is
    # sorted by match_date (oldest first).
    #
    # Watermark here isn't a date — it's the SET of event_ids already in
    # silver. New = any bronze record whose event_id isn't in that set.
    #
    # First run (no silver) => every record is "new" => full clean/build.
    #
    # Input:  bronze/openweather/saudi_league_matches.json
    # Output: silver/weather/cleaned_weather.csv

    import io
    import json
    import pandas as pd
    from azure.storage.filedatalake import DataLakeServiceClient

    # --- Storage access (Azure SDK, not Spark) ---
    storage_account = STORAGE_ACCOUNT

    storage_key = STORAGE_KEY

    service_client = DataLakeServiceClient(
        account_url=f"https://{storage_account}.dfs.core.windows.net",
        credential=storage_key,
    )

    bronze_fs = service_client.get_file_system_client(file_system=BRONZE_CONTAINER)
    silver_fs = service_client.get_file_system_client(file_system=SILVER_CONTAINER)

    SILVER_DIR = "weather"
    SILVER_FILE = "cleaned_weather.csv"

    WEATHER_NUMERIC_COLUMNS = [
        "home_score", "away_score", "latitude", "longitude",
        "temperature_2m", "apparent_temperature", "relative_humidity_2m",
        "dew_point_2m", "precipitation", "rain", "showers", "cloud_cover",
        "pressure_msl", "wind_speed_10m", "wind_direction_10m",
        "wind_gusts_10m", "visibility", "weather_code",
    ]


    def read_json(fs_client, path: str) -> dict:
        content = fs_client.get_file_client(path).download_file().readall()
        return json.loads(content)


    def read_existing_silver():
        """Return (df, set_of_event_ids) or (None, set()) for a full build."""
        try:
            content = silver_fs.get_file_client(f"{SILVER_DIR}/{SILVER_FILE}").download_file().readall()
            df = pd.read_csv(io.BytesIO(content))
            if df.empty or "event_id" not in df.columns:
                return df, set()
            return df, set(df["event_id"].astype(str))
        except Exception:
            print("No existing silver/weather file — this will be a full build.")
            return None, set()


    # ============================================================
    # PART 0 — existing silver + its event_id set (the "watermark")
    # ============================================================

    existing_df, existing_ids = read_existing_silver()
    print(f"event_ids already in silver: {len(existing_ids)}")


    # ============================================================
    # PART 1 — Load bronze, keep only records NOT already in silver
    # ============================================================

    try:
        data = read_json(bronze_fs, "openweather/saudi_league_matches.json")
    except Exception as e:
        dbutils.notebook.exit(
            f"Could not read bronze/openweather/saudi_league_matches.json ({e.__class__.__name__})."
        )

    df_bronze = pd.DataFrame(data)
    df_bronze["event_id"] = df_bronze["event_id"].astype(str)

    # The incremental filter: only rows whose event_id isn't already in silver.
    df_new = df_bronze[~df_bronze["event_id"].isin(existing_ids)].copy()
    print(f"Total bronze records: {len(df_bronze)}")
    print(f"New records to clean this run: {len(df_new)}")


    # ============================================================
    # PART 2 — Clean the new records (same steps as before)
    # ============================================================

    if not df_new.empty:
        dt_utc = pd.to_datetime(df_new["datetime_utc"], utc=True, errors="coerce")
        dt_local = dt_utc.dt.tz_convert("Asia/Riyadh")
        df_new["match_date"] = dt_local.dt.strftime("%Y-%m-%d")
        df_new["match_time"] = dt_local.dt.strftime("%H:%M:%S")

        df_new = df_new.drop(columns=["date", "match_datetime"], errors="ignore")

        for col in WEATHER_NUMERIC_COLUMNS:
            if col in df_new.columns:
                df_new[col] = pd.to_numeric(df_new[col], errors="coerce")

        df_new = df_new.drop_duplicates(subset=["event_id"])
    else:
        df_new = None
        print("Nothing new — silver is already up to date with bronze.")


    # ============================================================
    # PART 3 — Merge existing + new, dedup (new wins), sort
    # ============================================================

    existing_count = 0 if existing_df is None else len(existing_df)
    frames = [f for f in (existing_df, df_new) if f is not None and not f.empty]
    if not frames:
        dbutils.notebook.exit("No weather data at all — nothing to write.")

    combined = pd.concat(frames, ignore_index=True)
    combined["event_id"] = combined["event_id"].astype(str)
    combined = combined.drop_duplicates(subset=["event_id"], keep="last").reset_index(drop=True)

    # Sort chronologically (oldest first).
    if "match_date" in combined.columns:
        combined["_sort"] = pd.to_datetime(combined["match_date"], errors="coerce")
        combined = combined.sort_values("_sort").drop(columns="_sort").reset_index(drop=True)

    new_added = len(combined) - existing_count

    no_weather = combined["weather_code"].isna().sum() if "weather_code" in combined.columns else len(combined)


    # ============================================================
    # PART 4 — Save + summary
    # ============================================================

    silver_dir = silver_fs.get_directory_client(SILVER_DIR)
    silver_dir.create_directory()
    csv_bytes = combined.to_csv(index=False).encode("utf-8")
    silver_dir.create_file(SILVER_FILE).upload_data(csv_bytes, overwrite=True)

    print("\n==================== SILVER WEATHER SUMMARY ====================")
    print(f"Rows already in silver : {existing_count}")
    print(f"New/updated rows merged: {new_added if new_added > 0 else 0}")
    print(f"Total rows now         : {len(combined)}")
    print(f"Records with no weather: {no_weather}")
    print(f"Saved silver/{SILVER_DIR}/{SILVER_FILE}")
    print("===============================================================")
