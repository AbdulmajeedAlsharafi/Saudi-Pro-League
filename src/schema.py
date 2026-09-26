"""Pipeline stage extracted from 03_schema_validate.ipynb.
Existing notebook logic is preserved inside run_schema_validation().
"""

def run_schema_validation():
    import pandas as pd
    import numpy as np
    from pathlib import Path

    pd.set_option("display.max_columns", None)
    pd.set_option("display.max_colwidth", 100)


    # Temporary input location.
    # Final project paths will be:
    # data/silver/fixtures/cleaned_fixtures.csv
    # data/silver/match_stats/cleaned_stats.csv
    # data/silver/player_stats/cleaned_player_stats.csv
    # data/silver/match_events/cleaned_match_events.csv
    # data/silver/weather/cleaned_weather.csv
    # data/silver/h2h/cleaned_h2h.csv

    DATA_DIR = Path("/mnt/data")

    FILES = {
        "fixtures": DATA_DIR / "fixtures" / "cleaned_fixtures.csv",
        "match_stats": DATA_DIR / "match_stats" / "cleaned_stats.csv",
        "player_stats": DATA_DIR / "player_stats" / "cleaned_player_stats.csv",
        "match_events": DATA_DIR / "match_events" / "cleaned_match_events.csv",
        "weather": DATA_DIR / "weather" / "cleaned_weather.csv",
        "h2h": DATA_DIR / "h2h" / "cleaned_h2h.csv",
    }

    datasets = {
        "fixtures": pd.read_csv(FILES["fixtures"]),
        "match_stats": pd.read_csv(FILES["match_stats"]),
        "player_stats": pd.read_csv(FILES["player_stats"]),
        "match_events": pd.read_csv(FILES["match_events"]),
        "weather": pd.read_csv(FILES["weather"]),
        "h2h": pd.read_csv(FILES["h2h"]),
    }

    for name, df in datasets.items():
        print(f"{name:15} {df.shape}")


    def schema_rows(columns, dtype, nullable, rules=None):
        rules = rules or {}
        return [
            {
                "column": col,
                "data_type": dtype.get(col, "string"),
                "nullable": nullable.get(col, "Y"),
                "allowed_values_or_range": rules.get(col, "Any valid value"),
            }
            for col in columns
        ]


    fixtures_dtype = {
        "fixture_id": "integer", "fixture_referee": "string",
        "fixture_timezone": "string", "fixture_date": "datetime",
        "fixture_timestamp": "integer", "fixture_periods_first": "float",
        "fixture_periods_second": "float", "fixture_venue_id": "float",
        "fixture_venue_name": "string", "fixture_venue_city": "string",
        "fixture_status_long": "string", "fixture_status_short": "string",
        "fixture_status_elapsed": "integer", "fixture_status_extra": "float",
        "league_id": "integer", "league_name": "string",
        "league_country": "string", "league_season": "integer",
        "league_round": "string", "league_standings": "boolean",
        "teams_home_id": "integer", "teams_home_name": "string",
        "teams_home_winner": "boolean", "teams_away_id": "integer",
        "teams_away_name": "string", "teams_away_winner": "boolean",
        "goals_home": "integer", "goals_away": "integer",
        "score_halftime_home": "integer", "score_halftime_away": "integer",
        "score_fulltime_home": "integer", "score_fulltime_away": "integer",
    }

    fixtures_nullable = {
        col: "N" for col in fixtures.columns
    }
    for col in [
        "fixture_referee", "fixture_periods_first", "fixture_periods_second",
        "fixture_venue_id", "fixture_status_extra",
        "teams_home_winner", "teams_away_winner"
    ]:
        fixtures_nullable[col] = "Y"

    fixtures_rules = {
        "fixture_id": "> 0",
        "fixture_timestamp": "> 0",
        "league_id": "> 0",
        "league_season": "> 0",
        "teams_home_id": "> 0",
        "teams_away_id": "> 0",
        "goals_home": ">= 0",
        "goals_away": ">= 0",
        "score_halftime_home": ">= 0",
        "score_halftime_away": ">= 0",
        "score_fulltime_home": ">= 0",
        "score_fulltime_away": ">= 0",
        "league_standings": "True / False",
        "teams_home_winner": "True / False",
        "teams_away_winner": "True / False",
    }

    fixtures_schema = pd.DataFrame(
        schema_rows(fixtures.columns, fixtures_dtype, fixtures_nullable, fixtures_rules)
    )
    fixtures_schema


    match_stats_dtype = {col: "float" for col in match_stats.columns}
    match_stats_dtype.update({"fixture_id": "integer", "team": "string"})

    match_stats_nullable = {col: "Y" for col in match_stats.columns}
    match_stats_nullable.update({"fixture_id": "N", "team": "N"})

    match_stats_rules = {
        "fixture_id": "> 0",
        "team": "Non-empty team name",
        "shots_on_goal": ">= 0",
        "shots_off_goal": ">= 0",
        "total_shots": ">= 0",
        "blocked_shots": ">= 0",
        "shots_insidebox": ">= 0",
        "shots_outsidebox": ">= 0",
        "fouls": ">= 0",
        "corner_kicks": ">= 0",
        "offsides": ">= 0",
        "ball_possession": "0–100",
        "yellow_cards": ">= 0",
        "red_cards": ">= 0",
        "goalkeeper_saves": ">= 0",
        "total_passes": ">= 0",
        "passes_accurate": ">= 0",
        "passes_%": "0–100",
        "expected_goals": ">= 0",
    }

    match_stats_schema = pd.DataFrame(
        schema_rows(match_stats.columns, match_stats_dtype, match_stats_nullable, match_stats_rules)
    )
    match_stats_schema


    player_stats_dtype = {
        "fixture_id": "integer", "team_id": "integer", "player_id": "integer",
        "player_name": "string", "games_minutes": "float",
        "games_position": "string", "games_rating": "float",
        "games_captain": "boolean", "games_substitute": "boolean",
        "goals_total": "float", "goals_assists": "float", "goals_saves": "float",
        "shots_total": "float", "shots_on": "float",
        "passes_total": "float", "passes_key": "float",
        "passes_accuracy": "float", "tackles_total": "float",
        "tackles_blocks": "float", "tackles_interceptions": "float",
        "duels_total": "float", "duels_won": "float",
        "dribbles_attempts": "float", "dribbles_success": "float",
        "fouls_drawn": "float", "fouls_committed": "float",
        "cards_yellow": "integer", "cards_red": "integer",
        "penalty_won": "float", "penalty_scored": "integer",
        "penalty_missed": "float", "penalty_saved": "float",
    }

    player_stats_nullable = {col: "N" for col in player_stats.columns}
    for col in [
        "games_minutes", "games_position", "games_rating",
        "goals_total", "goals_assists", "goals_saves",
        "shots_total", "shots_on", "passes_total", "passes_key",
        "passes_accuracy", "tackles_total", "tackles_blocks",
        "tackles_interceptions", "duels_total", "duels_won",
        "dribbles_attempts", "dribbles_success", "fouls_drawn",
        "fouls_committed", "penalty_won", "penalty_missed",
        "penalty_saved"
    ]:
        player_stats_nullable[col] = "Y"

    player_stats_rules = {
        "fixture_id": "> 0",
        "team_id": "> 0",
        "player_id": "> 0",
        "player_name": "Non-empty player name",
        "games_minutes": ">= 0",
        "games_rating": "0–10 when present",
        "games_captain": "True / False",
        "games_substitute": "True / False",
        "goals_total": ">= 0 when present",
        "goals_assists": ">= 0 when present",
        "goals_saves": ">= 0 when present",
        "shots_total": ">= 0 when present",
        "shots_on": ">= 0 when present",
        "passes_total": ">= 0 when present",
        "passes_key": ">= 0 when present",
        "passes_accuracy": "0–100 when present",
        "tackles_total": ">= 0 when present",
        "tackles_blocks": ">= 0 when present",
        "tackles_interceptions": ">= 0 when present",
        "duels_total": ">= 0 when present",
        "duels_won": ">= 0 when present",
        "dribbles_attempts": ">= 0 when present",
        "dribbles_success": ">= 0 when present",
        "fouls_drawn": ">= 0 when present",
        "fouls_committed": ">= 0 when present",
        "cards_yellow": ">= 0",
        "cards_red": ">= 0",
        "penalty_won": ">= 0 when present",
        "penalty_scored": ">= 0",
        "penalty_missed": ">= 0 when present",
        "penalty_saved": ">= 0 when present",
    }

    player_stats_schema = pd.DataFrame(
        schema_rows(player_stats.columns, player_stats_dtype, player_stats_nullable, player_stats_rules)
    )
    player_stats_schema


    match_events_dtype = {
        "event_id": "string", "espn_match_id": "integer", "season": "string",
        "match_date": "datetime", "home_team_id": "integer",
        "home_team_name": "string", "away_team_id": "integer",
        "away_team_name": "string", "event_type_id": "integer",
        "event_type": "string", "event_seconds": "float",
        "event_minute": "string", "event_team_id": "integer",
        "player_id": "float", "player_name": "string",
        "player_position": "string", "score_value": "integer",
        "scoring_play": "boolean", "yellow_card": "boolean",
        "red_card": "boolean", "penalty_kick": "boolean",
        "own_goal": "boolean", "shootout": "boolean",
    }

    match_events_nullable = {col: "N" for col in match_events.columns}
    for col in ["player_id", "player_name", "player_position"]:
        match_events_nullable[col] = "Y"

    match_events_rules = {
        "event_id": "Non-empty",
        "espn_match_id": "> 0",
        "home_team_id": "> 0",
        "away_team_id": "> 0",
        "event_type_id": ">= 0",
        "event_seconds": ">= 0",
        "event_team_id": "> 0",
        "player_id": "> 0 when present",
        "score_value": ">= 0",
        "scoring_play": "True / False",
        "yellow_card": "True / False",
        "red_card": "True / False",
        "penalty_kick": "True / False",
        "own_goal": "True / False",
        "shootout": "True / False",
    }

    match_events_schema = pd.DataFrame(
        schema_rows(match_events.columns, match_events_dtype, match_events_nullable, match_events_rules)
    )
    match_events_schema


    weather_dtype = {
        "event_id": "integer", "season": "string", "datetime_utc": "datetime",
        "home_team": "string", "away_team": "string",
        "home_score": "integer", "away_score": "integer",
        "venue": "string", "venue_city": "string", "venue_country": "string",
        "home_club_city": "string", "away_club_city": "string",
        "match_date": "datetime", "match_time": "string",
        "latitude": "float", "longitude": "float",
        "temperature_2m": "float", "apparent_temperature": "float",
        "relative_humidity_2m": "integer", "dew_point_2m": "float",
        "precipitation": "float", "rain": "float", "showers": "float",
        "cloud_cover": "integer", "pressure_msl": "float",
        "wind_speed_10m": "float", "wind_direction_10m": "integer",
        "wind_gusts_10m": "float", "visibility": "integer",
        "weather_code": "integer", "weather_datetime": "integer",
        "weather_condition": "string",
    }

    weather_nullable = {col: "N" for col in weather.columns}

    weather_rules = {
        "event_id": "> 0",
        "home_score": ">= 0",
        "away_score": ">= 0",
        "latitude": "-90 to 90",
        "longitude": "-180 to 180",
        "relative_humidity_2m": "0–100",
        "precipitation": ">= 0",
        "rain": ">= 0",
        "showers": ">= 0",
        "cloud_cover": "0–100",
        "wind_direction_10m": "0–360",
        "visibility": ">= 0",
        "weather_code": ">= 0",
        "weather_datetime": "> 0",
    }

    weather_schema = pd.DataFrame(
        schema_rows(weather.columns, weather_dtype, weather_nullable, weather_rules)
    )
    weather_schema


    h2h_dtype = {
        "fixture_id": "integer", "fixture_referee": "string",
        "fixture_timezone": "string", "fixture_date": "datetime",
        "fixture_timestamp": "integer", "fixture_periods_first": "float",
        "fixture_periods_second": "float", "fixture_venue_id": "float",
        "fixture_venue_name": "string", "fixture_venue_city": "string",
        "fixture_status_long": "string", "fixture_status_short": "string",
        "fixture_status_elapsed": "float", "fixture_status_extra": "float",
        "league_id": "integer", "league_name": "string",
        "league_country": "string", "league_season": "integer",
        "league_round": "string", "league_standings": "boolean",
        "teams_home_id": "integer", "teams_home_name": "string",
        "teams_home_winner": "boolean", "teams_away_id": "integer",
        "teams_away_name": "string", "teams_away_winner": "boolean",
        "goals_home": "float", "goals_away": "float",
        "score_halftime_home": "float", "score_halftime_away": "float",
        "score_fulltime_home": "float", "score_fulltime_away": "float",
    }

    h2h_nullable = {col: "N" for col in h2h.columns}
    for col in [
        "fixture_referee", "fixture_periods_first", "fixture_periods_second",
        "fixture_venue_id", "fixture_venue_name", "fixture_venue_city",
        "fixture_status_elapsed", "fixture_status_extra",
        "teams_home_winner", "teams_away_winner",
        "goals_home", "goals_away",
        "score_halftime_home", "score_halftime_away",
        "score_fulltime_home", "score_fulltime_away"
    ]:
        h2h_nullable[col] = "Y"

    h2h_rules = {
        "fixture_id": "> 0",
        "fixture_timestamp": "> 0",
        "league_id": "> 0",
        "league_season": "> 0",
        "teams_home_id": "> 0",
        "teams_away_id": "> 0",
        "goals_home": ">= 0 when present",
        "goals_away": ">= 0 when present",
        "score_halftime_home": ">= 0 when present",
        "score_halftime_away": ">= 0 when present",
        "score_fulltime_home": ">= 0 when present",
        "score_fulltime_away": ">= 0 when present",
        "league_standings": "True / False",
        "teams_home_winner": "True / False when present",
        "teams_away_winner": "True / False when present",
    }

    h2h_schema = pd.DataFrame(
        schema_rows(h2h.columns, h2h_dtype, h2h_nullable, h2h_rules)
    )
    h2h_schema


    schemas = {
        "fixtures": fixtures_schema,
        "match_stats": match_stats_schema,
        "player_stats": player_stats_schema,
        "match_events": match_events_schema,
        "weather": weather_schema,
        "h2h": h2h_schema,
    }

    schema_table = pd.concat(
        [
            df.assign(dataset=name)
            for name, df in schemas.items()
        ],
        ignore_index=True
    )[["dataset", "column", "data_type", "nullable", "allowed_values_or_range"]]

    schema_table


    def add_reason(reasons, mask, message):
        reasons.loc[mask] = reasons.loc[mask].apply(
            lambda x: f"{x}; {message}" if x else message
        )


    def validate_dataset(df, schema, rules):
        working = df.copy()
        reasons = pd.Series("", index=working.index, dtype="object")

        # Required columns
        expected_columns = schema["column"].tolist()
        missing_columns = [c for c in expected_columns if c not in working.columns]
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")

        # Nullability
        for _, row in schema.iterrows():
            col = row["column"]
            if row["nullable"] == "N":
                add_reason(
                    reasons,
                    working[col].isna(),
                    f"{col} is null"
                )

        # Basic numeric rules
        non_negative = [
            c for c, rule in rules.items()
            if rule.startswith(">=")
        ]
        strictly_positive = [
            c for c, rule in rules.items()
            if rule.startswith("> 0")
        ]

        for col in non_negative:
            if col in working.columns:
                mask = working[col].notna() & (pd.to_numeric(working[col], errors="coerce") < 0)
                add_reason(reasons, mask, f"{col} is below minimum 0")

        for col in strictly_positive:
            if col in working.columns:
                numeric = pd.to_numeric(working[col], errors="coerce")
                mask = working[col].notna() & (numeric <= 0)
                add_reason(reasons, mask, f"{col} must be greater than 0")

        # Percentage/range rules
        for col in ["ball_possession", "passes_%", "passes_accuracy"]:
            if col in working.columns:
                numeric = pd.to_numeric(working[col], errors="coerce")
                mask = working[col].notna() & ~numeric.between(0, 100)
                add_reason(reasons, mask, f"{col} must be between 0 and 100")

        if "games_rating" in working.columns:
            numeric = pd.to_numeric(working["games_rating"], errors="coerce")
            mask = working["games_rating"].notna() & ~numeric.between(0, 10)
            add_reason(reasons, mask, "games_rating must be between 0 and 10")

        if "latitude" in working.columns:
            numeric = pd.to_numeric(working["latitude"], errors="coerce")
            mask = working["latitude"].notna() & ~numeric.between(-90, 90)
            add_reason(reasons, mask, "latitude must be between -90 and 90")

        if "longitude" in working.columns:
            numeric = pd.to_numeric(working["longitude"], errors="coerce")
            mask = working["longitude"].notna() & ~numeric.between(-180, 180)
            add_reason(reasons, mask, "longitude must be between -180 and 180")

        if "relative_humidity_2m" in working.columns:
            numeric = pd.to_numeric(working["relative_humidity_2m"], errors="coerce")
            mask = working["relative_humidity_2m"].notna() & ~numeric.between(0, 100)
            add_reason(reasons, mask, "relative_humidity_2m must be between 0 and 100")

        if "cloud_cover" in working.columns:
            numeric = pd.to_numeric(working["cloud_cover"], errors="coerce")
            mask = working["cloud_cover"].notna() & ~numeric.between(0, 100)
            add_reason(reasons, mask, "cloud_cover must be between 0 and 100")

        if "wind_direction_10m" in working.columns:
            numeric = pd.to_numeric(working["wind_direction_10m"], errors="coerce")
            mask = working["wind_direction_10m"].notna() & ~numeric.between(0, 360)
            add_reason(reasons, mask, "wind_direction_10m must be between 0 and 360")

        # Non-empty string checks for explicitly required textual identifiers
        for col in ["player_name", "team", "home_team_name", "away_team_name",
                    "home_team", "away_team", "event_id"]:
            if col in working.columns and schema.loc[schema["column"] == col, "nullable"].eq("N").any():
                mask = working[col].notna() & (working[col].astype(str).str.strip() == "")
                add_reason(reasons, mask, f"{col} is empty")

        working["rejection_reason"] = reasons
        rejected = working[working["rejection_reason"] != ""].copy()
        validated = working[working["rejection_reason"] == ""].drop(
            columns=["rejection_reason"]
        ).copy()

        return validated, rejected


    validation_results = {}

    for name, df in datasets.items():
        validated, rejected = validate_dataset(
            df,
            schemas[name],
            {
                row["column"]: row["allowed_values_or_range"]
                for _, row in schemas[name].iterrows()
            }
        )

        validation_results[name] = {
            "validated": validated,
            "rejected": rejected,
        }

    summary = pd.DataFrame([
        {
            "dataset": name,
            "input_rows": len(datasets[name]),
            "validated_rows": len(result["validated"]),
            "rejected_rows": len(result["rejected"]),
        }
        for name, result in validation_results.items()
    ])

    summary["validation_rate_pct"] = (
        summary["validated_rows"] / summary["input_rows"] * 100
    ).round(2)

    summary


    for name, result in validation_results.items():
        rejected = result["rejected"]

        print(f"\n{name.upper()}")
        print(f"Rejected rows: {len(rejected)}")

        if not rejected.empty:
            print(rejected["rejection_reason"].value_counts().head(20))


    # Temporary validation-output location.
    # Later, this can be moved to the project's designated validation-output folder.

    OUTPUT_DIR = DATA_DIR / "schema_validation_outputs"
    OUTPUT_DIR.mkdir(exist_ok=True)

    for name, result in validation_results.items():
        result["validated"].to_csv(
            OUTPUT_DIR / f"{name}_validated.csv",
            index=False
        )
        result["rejected"].to_csv(
            OUTPUT_DIR / f"{name}_rejected.csv",
            index=False
        )

    print(f"Validation outputs saved to: {OUTPUT_DIR}")


    # Basic notebook assertions
    assert set(datasets.keys()) == {
        "fixtures", "match_stats", "player_stats",
        "match_events", "weather", "h2h"
    }

    for name, result in validation_results.items():
        assert "rejection_reason" not in result["validated"].columns
        assert "rejection_reason" in result["rejected"].columns

    print("Schema validation notebook checks passed.")
