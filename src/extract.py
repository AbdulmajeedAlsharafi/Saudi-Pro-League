"""Pipeline stage extracted from 01_extract.ipynb.
Existing notebook logic is preserved inside run_extraction().
"""

def run_extraction():
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
    # extract_fixtures_and_stats_bronze
    # ============================================================
    # Finds SPL fixtures that finished TODAY, saves the raw fixtures
    # response, then pulls each fixture's team statistics and saves the
    # raw JSON.
    #
    # Incremental by design: each run asks the API about today only and
    # writes a new date folder. Folders from previous days are never read
    # or rewritten.
    #
    # Bronze = raw, unmodified data. No cleaning happens here.
    #
    # Output layout:
    #   bronze/fixtures/<date>/fixtures_<date>.json
    #   bronze/match_stats/<date>/fixture_<id>_statistics.json

    import requests
    import time
    import json
    from datetime import date
    from azure.storage.filedatalake import DataLakeServiceClient

    # --- Storage access (Azure SDK, not Spark) ---
    storage_account = STORAGE_ACCOUNT
    container = BRONZE_CONTAINER

    storage_key = STORAGE_KEY

    service_client = DataLakeServiceClient(
        account_url=f"https://{storage_account}.dfs.core.windows.net",
        credential=storage_key,
    )
    file_system_client = service_client.get_file_system_client(file_system=container)

    # --- API config ---
    API_KEY = API_FOOTBALL_KEY
    FIXTURES_URL = API_FOOTBALL_BASE_URL + CONFIG["apis"]["api_football"]["fixtures_endpoint"]
    FIXTURE_STATS_URL = API_FOOTBALL_BASE_URL + CONFIG["apis"]["api_football"]["fixture_stats_endpoint"]
    HEADERS = {"x-apisports-key": API_KEY}

    # Saudi Pro League. Update SEASON each year (API-Football uses the
    # starting year of the season, e.g. 2026 for the 2026-27 season).
    LEAGUE_ID = SPL_LEAGUE_ID
    SEASON = SPL_SEASON


    def get_json(url: str, params: dict, max_retries: int = MAX_RETRIES) -> dict:
        """GET with retry on timeout and rate limit. Raises if the API
        reports errors inside a 200 body (quota/suspension), so an error
        payload is never saved as if it were data."""
        response = None
        for attempt in range(max_retries):
            try:
                response = requests.get(url, headers=HEADERS, params=params, timeout=REQUEST_TIMEOUT)
            except requests.exceptions.Timeout:
                time.sleep(TIMEOUT_RETRY_SLEEP)
                continue
            if response.status_code == 429:
                time.sleep(RATE_LIMIT_SLEEP * (attempt + 1))
                continue
            break

        if response is None:
            raise RuntimeError(f"Failed after {max_retries} attempts ({params})")

        response.raise_for_status()
        data = response.json()

        if data.get("errors"):
            raise RuntimeError(f"API returned errors for {params}: {data['errors']}")

        return data


    def save_to_bronze(data: dict, file_name: str, dir_client) -> None:
        content = json.dumps(data, ensure_ascii=False).encode("utf-8")
        file_client = dir_client.create_file(file_name)
        file_client.upload_data(content, overwrite=True)


    # --- Step 1: find fixtures that finished today, save raw fixtures ---
    run_date = date.today().isoformat()
    fixtures_data = get_json(
        FIXTURES_URL,
        {"league": LEAGUE_ID, "season": SEASON, "date": run_date, "status": "FT"},
    )
    todays_fixtures = fixtures_data.get("response", [])
    fixture_ids = [f["fixture"]["id"] for f in todays_fixtures]

    print(f"Fixtures finished on {run_date}: {len(fixture_ids)}")

    if fixture_ids:
        fixtures_dir = file_system_client.get_directory_client(f"fixtures/{run_date}")
        fixtures_dir.create_directory()
        save_to_bronze(fixtures_data, f"fixtures_{run_date}.json", fixtures_dir)
        print(f"Saved raw fixtures to bronze/fixtures/{run_date}/")

    # --- Step 2: pull team statistics for each fixture, save to bronze ---
    stats_dir = file_system_client.get_directory_client(f"match_stats/{run_date}")
    if fixture_ids:
        stats_dir.create_directory()

    saved_count = 0
    no_stats_fixtures = []
    failed_fixtures = []

    for fid in fixture_ids:
        try:
            data = get_json(FIXTURE_STATS_URL, {"fixture": fid})
            if data.get("results", 0) > 0:
                save_to_bronze(data, f"fixture_{fid}_statistics.json", stats_dir)
                saved_count += 1
            else:
                # API has no stats for this match (happens for some fixtures).
                # Not saved; listed below so it is visible downstream.
                no_stats_fixtures.append(fid)
        except Exception as e:
            print(f"FAILED fixture {fid}: {e}")
            failed_fixtures.append(fid)
        time.sleep(BETWEEN_REQUESTS_SLEEP)

    print(f"Done: {saved_count}/{len(fixture_ids)} fixtures saved to bronze/match_stats/{run_date}/")
    if no_stats_fixtures:
        print(f"Fixtures with no stats from the API: {no_stats_fixtures}")
    if failed_fixtures:
        print(f"Failed fixtures: {failed_fixtures}")

    # ============================================================
    # extract_player_stats_bronze
    # ============================================================
    # Finds SPL fixtures that finished TODAY, then pulls each fixture's
    # player statistics and saves the raw JSON to Bronze.
    #
    # Incremental by design: each run asks the API about today only and
    # writes a new date folder. Folders from previous days are never read
    # or rewritten.
    #
    # Bronze = raw, unmodified data. No cleaning happens here.
    #
    # Output layout:
    #   bronze/match_player_stats/<date>/fixture_<id>_players.json

    import requests
    import time
    import json
    from datetime import date
    from azure.storage.filedatalake import DataLakeServiceClient

    # --- Storage access (Azure SDK, not Spark) ---
    storage_account = STORAGE_ACCOUNT
    container = BRONZE_CONTAINER

    storage_key = STORAGE_KEY

    service_client = DataLakeServiceClient(
        account_url=f"https://{storage_account}.dfs.core.windows.net",
        credential=storage_key,
    )
    file_system_client = service_client.get_file_system_client(file_system=container)

    # --- API config ---
    API_KEY = API_FOOTBALL_KEY
    FIXTURES_URL = API_FOOTBALL_BASE_URL + CONFIG["apis"]["api_football"]["fixtures_endpoint"]
    FIXTURE_PLAYERS_URL = API_FOOTBALL_BASE_URL + CONFIG["apis"]["api_football"]["fixture_players_endpoint"]
    HEADERS = {"x-apisports-key": API_KEY}

    # Saudi Pro League. Update SEASON each year (API-Football uses the
    # starting year of the season, e.g. 2026 for the 2026-27 season).
    LEAGUE_ID = SPL_LEAGUE_ID
    SEASON = SPL_SEASON


    def get_json(url: str, params: dict, max_retries: int = MAX_RETRIES) -> dict:
        """GET with retry on timeout and rate limit. Raises if the API
        reports errors inside a 200 body (quota/suspension), so an error
        payload is never saved as if it were data."""
        response = None
        for attempt in range(max_retries):
            try:
                response = requests.get(url, headers=HEADERS, params=params, timeout=REQUEST_TIMEOUT)
            except requests.exceptions.Timeout:
                time.sleep(TIMEOUT_RETRY_SLEEP)
                continue
            if response.status_code == 429:
                time.sleep(RATE_LIMIT_SLEEP * (attempt + 1))
                continue
            break

        if response is None:
            raise RuntimeError(f"Failed after {max_retries} attempts ({params})")

        response.raise_for_status()
        data = response.json()

        if data.get("errors"):
            raise RuntimeError(f"API returned errors for {params}: {data['errors']}")

        return data


    def save_to_bronze(data: dict, file_name: str, dir_client) -> None:
        content = json.dumps(data, ensure_ascii=False).encode("utf-8")
        file_client = dir_client.create_file(file_name)
        file_client.upload_data(content, overwrite=True)


    # --- Step 1: find fixtures that finished today ---
    run_date = date.today().isoformat()
    fixtures_data = get_json(
        FIXTURES_URL,
        {"league": LEAGUE_ID, "season": SEASON, "date": run_date, "status": "FT"},
    )
    todays_fixtures = fixtures_data.get("response", [])
    fixture_ids = [f["fixture"]["id"] for f in todays_fixtures]

    print(f"Fixtures finished on {run_date}: {len(fixture_ids)}")

    # --- Step 2: pull player stats for each fixture, save to bronze ---
    dir_client = file_system_client.get_directory_client(f"match_player_stats/{run_date}")
    if fixture_ids:
        dir_client.create_directory()

    saved_count = 0
    failed_fixtures = []

    for fid in fixture_ids:
        try:
            data = get_json(FIXTURE_PLAYERS_URL, {"fixture": fid})
            save_to_bronze(data, f"fixture_{fid}_players.json", dir_client)
            saved_count += 1
        except Exception as e:
            print(f"FAILED fixture {fid}: {e}")
            failed_fixtures.append(fid)
        time.sleep(BETWEEN_REQUESTS_SLEEP)

    print(f"Done: {saved_count}/{len(fixture_ids)} fixtures saved to bronze/match_player_stats/{run_date}/")
    if failed_fixtures:
        print(f"Failed fixtures: {failed_fixtures}")

    # ============================================================
    # CELL  — main extraction code
    # ============================================================
    # Databricks notebook: extract_match_events_bronze
    # Purpose: pull the ESPN Saudi Pro League scoreboard for TODAY and save
    # the raw JSON to Bronze. Match events (goals, cards, etc.) live inside
    # each event's competitions[0].details, so the full response is kept
    # untouched and parsed later in Silver.
    # Bronze = raw, unmodified data. No cleaning happens here.
    #
    # Output layout:
    #   bronze/match_events/<date>/scoreboard_<YYYYMMDD>.json

    import requests
    import time
    import json
    from datetime import date
    from azure.storage.filedatalake import DataLakeServiceClient

    # --- Storage access (Azure SDK, not Spark) ---
    storage_account = STORAGE_ACCOUNT
    container = BRONZE_CONTAINER

    storage_key = STORAGE_KEY

    service_client = DataLakeServiceClient(
        account_url=f"https://{storage_account}.dfs.core.windows.net",
        credential=storage_key,
    )
    file_system_client = service_client.get_file_system_client(file_system=container)

    # --- ESPN config (public endpoint, no API key needed) ---
    SCOREBOARD_URL = ESPN_SCOREBOARD_URL


    def get_json(url: str, params: dict, max_retries: int = MAX_RETRIES) -> dict:
        response = None
        for attempt in range(max_retries):
            try:
                response = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
            except requests.exceptions.Timeout:
                time.sleep(TIMEOUT_RETRY_SLEEP)
                continue
            if response.status_code == 429:
                time.sleep(RATE_LIMIT_SLEEP * (attempt + 1))
                continue
            break

        if response is None:
            raise RuntimeError(f"Failed after {max_retries} attempts ({params})")

        response.raise_for_status()
        return response.json()


    def save_to_bronze(data: dict, file_name: str, dir_client) -> None:
        content = json.dumps(data, ensure_ascii=False).encode("utf-8")
        file_client = dir_client.create_file(file_name)
        file_client.upload_data(content, overwrite=True)


    # --- Step 1: fetch today's scoreboard ---
    run_date = date.today().isoformat()
    date_str = date.today().strftime("%Y%m%d")

    data = get_json(SCOREBOARD_URL, {"dates": date_str})
    events = data.get("events", [])
    completed = [
        e for e in events
        if e.get("status", {}).get("type", {}).get("completed")
    ]

    print(f"ESPN events on {run_date}: {len(events)} ({len(completed)} completed)")

    # --- Step 2: save the raw response only if there are finished matches ---
    if completed:
        dir_client = file_system_client.get_directory_client(f"match_events/{run_date}")
        dir_client.create_directory()
        save_to_bronze(data, f"scoreboard_{date_str}.json", dir_client)
        print(f"Done: saved bronze/match_events/{run_date}/scoreboard_{date_str}.json")
        if len(completed) < len(events):
            print("WARNING: some matches were not finished at run time. "
                  "Re-run later to overwrite with the final data.")
    else:
        print(f"Done: no finished matches on {run_date}, nothing saved")


    # ============================================================
    # CELL  — main extraction code
    # ============================================================
    # Databricks notebook: extract_h2h_bronze
    # Purpose: save the raw head-to-head history for every SPL pair that
    # actually played TODAY.
    #
    # Pairs now come from today's finished fixtures, not from every
    # possible team combination. The previous version looped over all
    # C(18,2) = 153 pairs and called the H2H endpoint once per pair —
    # about 3 minutes and 153 API calls every day, to find the 2-3
    # matches that were actually played. This version spends 1 call
    # listing today's fixtures plus one call per real match, so a normal
    # matchday costs 4 calls and a few seconds.
    #
    # Bronze = raw, unmodified data. No cleaning happens here.
    #
    # Output layout (unchanged):
    #   bronze/h2h/<date>/h2h_<team1>_<team2>_<date>.json

    import requests
    import time
    import json
    from datetime import date
    from azure.storage.filedatalake import DataLakeServiceClient

    # --- Storage access (Azure SDK, not Spark) ---
    storage_account = STORAGE_ACCOUNT
    container = BRONZE_CONTAINER

    storage_key = STORAGE_KEY

    service_client = DataLakeServiceClient(
        account_url=f"https://{storage_account}.dfs.core.windows.net",
        credential=storage_key,
    )
    file_system_client = service_client.get_file_system_client(file_system=container)

    # --- API config ---
    API_KEY = API_FOOTBALL_KEY
    FIXTURES_URL = API_FOOTBALL_BASE_URL + CONFIG["apis"]["api_football"]["fixtures_endpoint"]
    H2H_URL = API_FOOTBALL_BASE_URL + CONFIG["apis"]["api_football"]["h2h_endpoint"]
    HEADERS = {"x-apisports-key": API_KEY}

    # Saudi Pro League. Update SEASON each year (API-Football uses the
    # starting year of the season, e.g. 2026 for the 2026-27 season).
    LEAGUE_ID = SPL_LEAGUE_ID
    SEASON = SPL_SEASON


    def get_json(url: str, params: dict, max_retries: int = MAX_RETRIES) -> dict:
        """GET with retry on timeout and rate limit. Raises if the API
        reports errors inside a 200 body (quota/suspension), so an error
        payload is never saved as if it were data."""
        response = None
        for attempt in range(max_retries):
            try:
                response = requests.get(url, headers=HEADERS, params=params, timeout=REQUEST_TIMEOUT)
            except requests.exceptions.Timeout:
                time.sleep(TIMEOUT_RETRY_SLEEP)
                continue
            if response.status_code == 429:
                time.sleep(RATE_LIMIT_SLEEP * (attempt + 1))
                continue
            break

        if response is None:
            raise RuntimeError(f"Failed after {max_retries} attempts ({params})")

        response.raise_for_status()
        data = response.json()

        if data.get("errors"):
            raise RuntimeError(f"API returned errors for {params}: {data['errors']}")

        return data


    def save_to_bronze(data: dict, team1_id: int, team2_id: int, match_date: str, dir_client) -> None:
        # File name includes the date so a later match between the same two
        # teams never overwrites an earlier one's raw file.
        file_name = f"h2h_{team1_id}_{team2_id}_{match_date}.json"
        content = json.dumps(data, ensure_ascii=False).encode("utf-8")
        file_client = dir_client.create_file(file_name)
        file_client.upload_data(content, overwrite=True)


    # --- Step 1: find out which teams actually played today ---
    run_date = date.today().isoformat()

    fixtures_data = get_json(
        FIXTURES_URL,
        {"league": LEAGUE_ID, "season": SEASON, "date": run_date, "status": "FT"},
    )
    todays_fixtures = fixtures_data.get("response", [])

    # Each pair is sorted so the same match always produces the same file
    # name, whichever side happened to be at home.
    pairs_played = []
    for fixture in todays_fixtures:
        teams = fixture.get("teams", {})
        home_id = teams.get("home", {}).get("id")
        away_id = teams.get("away", {}).get("id")
        if home_id and away_id:
            pairs_played.append(tuple(sorted((home_id, away_id))))

    pairs_played = sorted(set(pairs_played))

    print(f"Fixtures finished on {run_date}: {len(todays_fixtures)}")
    print(f"Team pairs to fetch H2H for: {len(pairs_played)}")

    # --- Step 2: fetch H2H for those pairs only ---
    if not pairs_played:
        print(f"No matches on {run_date} — nothing to save. (1 API call used)")
    else:
        # The date folder is created lazily — only when the first real match
        # is saved — so a day with no matches leaves no empty folder behind.
        dir_client = file_system_client.get_directory_client(f"h2h/{run_date}")

        saved_count = 0
        empty_count = 0
        failed_pairs = []
        dir_created = False

        for team1_id, team2_id in pairs_played:
            params = {"h2h": f"{team1_id}-{team2_id}", "date": run_date}
            try:
                data = get_json(H2H_URL, params)

                # Same payload shape the old version saved, so the silver
                # notebook keeps reading these files unchanged.
                payload = {
                    "get": "fixtures/headtohead",
                    "parameters": params,
                    "errors": data.get("errors"),
                    "results": data.get("results", 0),
                    "response": data.get("response", []),
                }

                if payload["results"] > 0:
                    if not dir_created:
                        dir_client.create_directory()
                        dir_created = True
                    save_to_bronze(payload, team1_id, team2_id, run_date, dir_client)
                    saved_count += 1
                else:
                    # The fixtures list says they played, but H2H returned
                    # nothing for that date — worth logging, since it
                    # shouldn't normally happen.
                    print(f"NOTE: no H2H result for {team1_id}-{team2_id} on {run_date}")
                    empty_count += 1
            except Exception as e:
                print(f"FAILED {team1_id}-{team2_id}: {e}")
                failed_pairs.append((team1_id, team2_id))
            time.sleep(0.5)

        print(f"\nDone: {saved_count} match(es) saved to bronze/h2h/{run_date}/")
        if empty_count:
            print(f"Pairs that returned no H2H data: {empty_count}")
        if failed_pairs:
            print(f"Failed pairs: {failed_pairs}")
        print(f"API calls used this run: {1 + len(pairs_played)} (old version: 153)")

    # ============================================================
    # extract_weather_bronze
    # ============================================================
    # Builds the SPL match list from ESPN, then fetches the weather at
    # each match's venue city and kickoff time from Open-Meteo.
    #
    # Incremental by design, same contract as the other bronze notebooks:
    # ask the source about TODAY only, and write only what isn't stored
    # yet. The difference is the storage shape — this source is a single
    # cumulative file rather than date folders, so "already stored" is
    # decided by event_id instead of by folder name. Records already in
    # the file are never refetched or rewritten.
    #
    # bronze/openweather/saudi_league_matches.json also holds a
    # teammate's manually-built match+weather dataset; this notebook
    # loads that exact file and appends to it, so there stays one single
    # weather source of truth.
    #
    # Bronze = raw, unmodified data. No cleaning happens here.
    #
    # Output layout (single file, appended to each run):
    #   bronze/openweather/saudi_league_matches.json

    import requests
    import time
    import json
    import pandas as pd
    from datetime import date, timedelta
    from azure.storage.filedatalake import DataLakeServiceClient

    # --- Storage access (Azure SDK, not Spark) ---
    storage_account = STORAGE_ACCOUNT
    container = BRONZE_CONTAINER

    storage_key = STORAGE_KEY

    service_client = DataLakeServiceClient(
        account_url=f"https://{storage_account}.dfs.core.windows.net",
        credential=storage_key,
    )
    file_system_client = service_client.get_file_system_client(file_system=container)

    # --- ESPN config (public endpoint, no API key needed) ---
    # --- ESPN config (public endpoint, no API key needed) ---
    # Loaded from config.yaml in the shared configuration cell.

    # --- Open-Meteo config (public endpoint, no API key needed) ---
    # Loaded from config.yaml in the shared configuration cell.

    # Historical rebuild window — only used when FULL_REBUILD is True.
    START_DATE = pd.Timestamp(CONFIG["weather"]["start_date"])

    # Normal daily behaviour is controlled by config.yaml.
    FULL_REBUILD = CONFIG["weather"]["full_rebuild"]

    WEATHER_VARIABLES = [
        "temperature_2m",
        "apparent_temperature",
        "relative_humidity_2m",
        "dew_point_2m",
        "precipitation",
        "rain",
        "showers",
        "cloud_cover",
        "pressure_msl",
        "wind_speed_10m",
        "wind_direction_10m",
        "wind_gusts_10m",
        "visibility",
        "weather_code",
    ]

    # WMO weather codes -> short description (matches the existing manual
    # dataset's "weather_condition" field). Codes not listed fall back to
    # "Unknown" rather than crashing.
    WMO_CODE_TEXT = {
        0: "Clear sky", 1: "Mainly clear", 2: "Partly cloudy", 3: "Overcast",
        45: "Fog", 48: "Depositing rime fog",
        51: "Light drizzle", 53: "Moderate drizzle", 55: "Dense drizzle",
        56: "Light freezing drizzle", 57: "Dense freezing drizzle",
        61: "Slight rain", 63: "Moderate rain", 65: "Heavy rain",
        66: "Light freezing rain", 67: "Heavy freezing rain",
        71: "Slight snow fall", 73: "Moderate snow fall", 75: "Heavy snow fall",
        77: "Snow grains",
        80: "Slight rain showers", 81: "Moderate rain showers", 82: "Violent rain showers",
        85: "Slight snow showers", 86: "Heavy snow showers",
        95: "Thunderstorm", 96: "Thunderstorm with slight hail", 99: "Thunderstorm with heavy hail",
    }

    # --- Club city + coordinates (single source of truth) ---
    CITY_COORDS = {
        "Riyadh": (24.7136, 46.6753),
        "Jeddah": (21.4858, 39.1925),
        "Dammam": (26.4207, 50.0888),
        "Buraidah": (26.3592, 43.9818),
        "Al Majma'ah": (25.9100, 45.3567),
        "Al-Hofuf": (25.3830, 49.5860),
        "Al-Rass": (25.8694, 43.4973),
        "Abha": (18.2164, 42.5053),
        "Khamis Mushait": (18.3000, 42.7333),
        "Najran": (17.4933, 44.1277),
        "Mecca": (21.3891, 39.8579),
        "Tabuk": (28.3838, 36.5550),
        "Al-Jawf": (29.8870, 39.3208),
        "Hafar Al-Batin": (28.4328, 45.9708),
        "Khobar": (26.2172, 50.1971),
    }

    # ESPN venue names that don't map 1:1 to a city name above
    VENUE_CITY_OVERRIDES = {
        "King Fahd International Stadium": "Riyadh",
    }

    TEAM_NAME_FIXES = {
        "Al-Ahli": "Al Ahli", "Al-Ittihad": "Al Ittihad", "Al-Nassr": "Al Nassr",
        "Al-Hilal": "Al Hilal", "Al-Shabab": "Al Shabab", "Al-Taawoun": "Al Taawoun",
        "Al-Raed": "Al Raed", "Al-Fateh": "Al Fateh", "Al-Faisaly": "Al Faisaly",
        "Al-Fayha": "Al Fayha", "Al-Wehda": "Al Wehda", "Al-Ettifaq": "Al Ettifaq",
        "Al-Batin": "Al Batin", "Al-Hazem": "Al Hazem", "Al-Qadsiah": "Al Qadsiah",
        "Al-Khaleej": "Al Khaleej", "Al-Kholood": "Al Kholood", "Al-Tai": "Al Tai",
        "Al-Akhdoud": "Al Akhdoud", "Al-Okhdood": "Al Okhdood", "Al-Orubah": "Al Orubah",
        "Al-Adalah": "Al Adalah", "Al-Riyadh": "Al Riyadh", "Al-Najma": "Al Najma",
        "NEOM SC": "NEOM", "Neom": "NEOM",
    }


    def normalize_team_name(name: str) -> str:
        return TEAM_NAME_FIXES.get(name, name)


    def get_season(match_date: pd.Timestamp) -> str:
        if match_date.month >= 8:
            return f"{match_date.year}-{str(match_date.year + 1)[-2:]}"
        return f"{match_date.year - 1}-{str(match_date.year)[-2:]}"


    def get_matches_from_espn(start_date, end_date, max_retries: int = MAX_RETRIES) -> list:
        """Walk ESPN's scoreboard day by day and collect finished matches
        with venue/city info. Returns a list of dicts (raw match records)."""
        all_matches = []
        current_date = start_date

        while current_date <= end_date:
            date_str = current_date.strftime("%Y%m%d")
            response = None

            for attempt in range(max_retries):
                try:
                    response = requests.get(
                        ESPN_SCOREBOARD_URL, params={"dates": date_str}, timeout=REQUEST_TIMEOUT
                    )
                except requests.exceptions.Timeout:
                    time.sleep(TIMEOUT_RETRY_SLEEP)
                    continue
                if response.status_code == 429:
                    time.sleep(RATE_LIMIT_SLEEP * (attempt + 1))
                    continue
                break

            if response is None or response.status_code != 200:
                print(f"ERROR {date_str}: request failed after retries")
                current_date += timedelta(days=1)
                continue

            data = response.json()
            events = data.get("events", [])

            for event in events:
                competitions = event.get("competitions", [])
                if not competitions:
                    continue
                competition = competitions[0]
                competitors = competition.get("competitors", [])
                if len(competitors) < 2:
                    continue

                home = away = home_score = away_score = None
                for team in competitors:
                    team_name = normalize_team_name(
                        team.get("team", {}).get("displayName")
                    )
                    score = team.get("score")
                    if team.get("homeAway") == "home":
                        home, home_score = team_name, score
                    elif team.get("homeAway") == "away":
                        away, away_score = team_name, score

                if not home or not away:
                    continue

                venue = competition.get("venue", {})
                address = venue.get("address", {})
                venue_city = address.get("city")
                venue_city = VENUE_CITY_OVERRIDES.get(venue_city, venue_city)

                all_matches.append({
                    "event_id": event.get("id"),
                    "season": get_season(current_date),
                    "date": current_date.strftime("%Y-%m-%d"),
                    "datetime_utc": event.get("date"),
                    "home_team": home,
                    "away_team": away,
                    "home_score": home_score,
                    "away_score": away_score,
                    "venue": venue.get("fullName"),
                    "venue_city": venue_city,
                    "venue_country": address.get("country"),
                })

            print(f"{date_str} -> {len(events)} events")
            current_date += timedelta(days=1)
            time.sleep(0.05)

        return all_matches


    def get_weather_for_match(latitude, longitude, match_date, match_time, max_retries: int = 3):
        """Fetch hourly weather for match_date and return the hour closest
        to kickoff, as a FLAT dict matching the existing dataset's schema
        (temperature_2m, weather_code, weather_condition, ...). Returns {}
        if the city has no known coordinates or the API call fails after
        retries — never None, so it merges cleanly with `|`."""
        if latitude is None or longitude is None:
            return {}

        params = {
            "latitude": latitude,
            "longitude": longitude,
            "start_date": match_date,
            "end_date": match_date,
            "hourly": ",".join(WEATHER_VARIABLES),
            "timezone": TIMEZONE,
        }

        hourly = None
        for attempt in range(max_retries):
            try:
                response = requests.get(OPEN_METEO_URL, params=params, timeout=60)
                response.raise_for_status()
                hourly = response.json().get("hourly")
                break
            except (requests.exceptions.RequestException, json.JSONDecodeError, ValueError) as e:
                # Covers network errors AND a 200 response with an empty/bad
                # body (a JSONDecodeError once killed a whole run).
                print(f"    Attempt {attempt + 1}/{max_retries} failed: {e}")
                time.sleep(5 * (attempt + 1))
                hourly = None

        if not hourly:
            return {}

        hourly_df = pd.DataFrame(hourly)
        hourly_df["time"] = pd.to_datetime(hourly_df["time"])
        match_datetime = pd.to_datetime(f"{match_date} {match_time}")
        hourly_df["time_difference"] = abs(hourly_df["time"] - match_datetime)
        closest = hourly_df.loc[hourly_df["time_difference"].idxmin()]

        weather = {var: closest[var] for var in WEATHER_VARIABLES}
        weather["weather_datetime"] = str(closest["time"])
        weather["weather_condition"] = WMO_CODE_TEXT.get(int(closest["weather_code"]), "Unknown")
        return weather


    def save_to_bronze(data, file_name: str, dir_client) -> None:
        content = json.dumps(data, ensure_ascii=False, default=str).encode("utf-8")
        file_client = dir_client.create_file(file_name)
        file_client.upload_data(content, overwrite=True)


    def load_existing_weather(dir_client, file_name: str) -> list:
        """Read the master weather file from bronze if it already exists.
        Returns an empty list on first run (file not found)."""
        try:
            file_client = dir_client.get_file_client(file_name)
            downloaded = file_client.download_file()
            content = downloaded.readall()
            existing = json.loads(content)
            print(f"Loaded {len(existing)} existing weather records from bronze.")
            return existing
        except Exception:
            print("No existing weather file found in bronze — starting fresh.")
            return []


    WEATHER_DIR = "openweather"
    WEATHER_FILE = "saudi_league_matches.json"

    dir_client = file_system_client.get_directory_client(WEATHER_DIR)
    dir_client.create_directory()  # no-op if it already exists

    # --- Step 1: load what's already fetched, so we know what to skip ---
    existing_records = load_existing_weather(dir_client, WEATHER_FILE)
    existing_event_ids = {str(r["event_id"]) for r in existing_records}

    # --- Step 2: scan ESPN for today only ---
    today = pd.Timestamp(date.today().isoformat())

    if FULL_REBUILD:
        scan_start = START_DATE
        print(f"FULL REBUILD — scanning from {scan_start.date()} to {today.date()}")
    else:
        scan_start = today
        print(f"Scanning ESPN for {today.date()}")

    matches = get_matches_from_espn(scan_start, today)
    print(f"\nMatches found: {len(matches)}")

    # --- Step 3: keep only matches we haven't fetched weather for yet ---
    if not matches:
        df = pd.DataFrame(columns=["event_id"])
    else:
        df = pd.DataFrame(matches)
        df = df[~df["event_id"].astype(str).isin(existing_event_ids)].reset_index(drop=True)

    print(f"New matches to fetch weather for: {len(df)}")

    if df.empty:
        print(f"Nothing new to fetch. bronze/{WEATHER_DIR}/{WEATHER_FILE} is already up to date.")
    else:
        # --- Step 4: derive local match_date / match_time from datetime_utc ---
        dt_utc = pd.to_datetime(df["datetime_utc"], utc=True, errors="coerce")
        dt_local = dt_utc.dt.tz_convert(TIMEZONE)
        df["match_date"] = dt_local.dt.strftime("%Y-%m-%d")
        df["match_time"] = dt_local.dt.strftime("%H:%M:%S")

        # --- Step 5: attach lat/lon from venue_city ---
        df["latitude"] = df["venue_city"].map(lambda c: CITY_COORDS.get(c, (None, None))[0])
        df["longitude"] = df["venue_city"].map(lambda c: CITY_COORDS.get(c, (None, None))[1])

        unmapped_cities = sorted(df.loc[df["latitude"].isna(), "venue_city"].dropna().unique())
        if unmapped_cities:
            print(f"WARNING: no coordinates for these venue cities, weather will be null: {unmapped_cities}")

        # --- Step 6: fetch weather for the new matches, merged flat ---
        # Save progress periodically so a mid-run failure never costs more
        # than SAVE_EVERY matches of work.
        SAVE_EVERY = 100
        new_records = []
        for i, row in df.iterrows():
            print(f"[{i + 1}/{len(df)}] {row['event_id']} | {row['venue_city']} | {row['match_date']} {row['match_time']}")
            weather = get_weather_for_match(
                row["latitude"], row["longitude"], row["match_date"], row["match_time"]
            )
            record = row.to_dict() | weather
            new_records.append(record)
            time.sleep(0.2)

            if len(new_records) % SAVE_EVERY == 0:
                save_to_bronze(existing_records + new_records, WEATHER_FILE, dir_client)
                print(f"    ...checkpoint saved ({len(existing_records) + len(new_records)} total records)")

        with_weather = sum(1 for r in new_records if r.get("weather_code") is not None)
        print("\n======================================")
        print("WEATHER EXTRACTION COMPLETE")
        print("======================================")
        print(f"New matches fetched: {len(new_records)}")
        print(f"New matches with weather: {with_weather}")
        print(f"New matches without weather: {len(new_records) - with_weather}")

        # --- Step 7: merge with what was already there and save back ---
        all_records = existing_records + new_records
        save_to_bronze(all_records, WEATHER_FILE, dir_client)
        print(f"\nSaved bronze/{WEATHER_DIR}/{WEATHER_FILE} — {len(all_records)} total records "
              f"({len(existing_records)} existing + {len(new_records)} new)")
