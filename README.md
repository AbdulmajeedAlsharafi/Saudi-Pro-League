# Saudi Pro League Data Engineering Pipeline

## 1. Project Overview

This project is an end-to-end data engineering pipeline for Saudi Pro League football data.

The pipeline extracts data from football and weather APIs, stores and processes the data through a Medallion Architecture, validates the datasets against defined schemas, and produces analysis-ready Gold Delta tables.

The project is organized into four main pipeline stages:

```text
APIs
  ↓
01_extract.ipynb
  ↓
Bronze
  ↓
02_profile_clean.ipynb
  ↓
Silver
  ↓
03_schema_validate.ipynb
  ↓
04_join_transform.ipynb
  ↓
Gold
```

## 2. Architecture

The project uses a Medallion Architecture:

- **Bronze:** raw API responses.
- **Silver:** flattened, cleaned, and standardized datasets.
- **Schema Validation:** validates individual Silver datasets against their declared schemas.
- **Gold:** joins and transforms the validated data into relational Delta tables and performs PK/FK validation.

### Repository Data Mapping

The assignment uses the generic terms `raw`, `interim`, and `processed`. This project uses the existing Medallion terminology instead:

| Assignment terminology | Project terminology |
|---|---|
| `data/raw/` | `data/bronze/` |
| `data/interim/` | `data/silver/` |
| `data/processed/` | `data/gold/` |

## 3. Repository Structure

```text
Saudi-Pro-League/
├── .gitignore
├── requirements.txt
├── README.md
├── config.yaml
├── main.py
│
├── data/
│   ├── bronze/
│   ├── silver/
│   └── gold/
│
├── notebooks/
│   ├── 01_extract.ipynb
│   ├── 02_profile_clean.ipynb
│   ├── 03_schema_validate.ipynb
│   └── 04_join_transform.ipynb
│
├── src/
│   ├── __init__.py
│   ├── config.py
│   ├── extract.py
│   ├── profile.py
│   ├── clean.py
│   ├── schema.py
│   └── transform.py
│
└── tests/
    ├── test_clean.py
    └── test_transform.py
```

The `data/` directory follows the project's Bronze/Silver/Gold structure.

## 4. Data Sources

### API-Football

Used for:

- Fixtures
- Match statistics
- Player statistics
- Head-to-head data

Base URL:

```text
https://v3.football.api-sports.io
```

### ESPN

Used for match event data.

```text
https://site.api.espn.com/apis/site/v2/sports/soccer/ksa.1/scoreboard
```

### Open-Meteo

Used for historical weather data associated with matches.

```text
https://historical-forecast-api.open-meteo.com/v1/forecast
```

## 5. Authentication and Secrets

Sensitive credentials are not stored in the repository.

The pipeline retrieves credentials through Databricks Secrets:

```text
Secret scope: spl-scope
```

Used secret keys include:

```text
api-football-key
storage-account-key
```

The actual secret values must not be placed in:

- `config.yaml`
- notebooks
- source code
- README
- GitHub

## 6. Configuration

Non-sensitive configuration is stored in:

```text
config.yaml
```

This includes configuration such as:

- Azure storage information
- Bronze and Silver container names
- League ID
- Season
- API endpoints
- Request settings

Secrets remain in Databricks Secrets.

## 7. Pipeline Notebooks

### 01 — Extract

`notebooks/01_extract.ipynb`

Extracts data from the project APIs and writes raw responses to the Bronze layer.

Bronze datasets include:

```text
data/bronze/
├── fixtures/
├── h2h/
├── match_events/
├── match_player_stats/
├── match_stats/
└── openweather/
```

### 02 — Profile and Clean

`notebooks/02_profile_clean.ipynb`

Reads Bronze data, profiles the datasets, identifies data-quality issues, flattens nested data where required, and cleans the resulting datasets.

Silver datasets include:

```text
data/silver/
├── fixtures/
├── h2h/
├── match_events/
├── match_stats/
├── player_stats/
└── weather/
```

### 03 — Schema Validation

`notebooks/03_schema_validate.ipynb`

Validates the six Silver datasets against declared schemas.

Each schema defines:

- Column name
- Expected data type
- Nullable status
- Allowed values or range

Rows that fail validation are routed to rejected output with a rejection reason. Schema validation is performed at the individual dataset level.

Primary-key and foreign-key relational validation is handled in the Gold transformation stage.

### 04 — Join and Transform

`notebooks/04_join_transform.ipynb`

Combines the validated Silver datasets and produces the Gold relational model.

The transformation includes:

- Team-name standardization
- Team lookup
- `fixture_id → match_id` mapping
- Team → `team_id` mapping
- Match-event resolution
- Weather resolution
- H2H transformation
- Primary-key validation
- Foreign-key validation
- Incremental Delta MERGE/upsert

## 8. Gold Data Model

The Gold layer contains six main tables.

| Gold table | Grain | Primary Key |
|---|---|---|
| `match_stats` | One row per team per match | `(match_id, team_id)` |
| `teams` | One row per team | `team_id` |
| `player_stats` | One row per player per match | `(match_id, player_id)` |
| `match_events` | One row per match event | `event_id` |
| `weather` | One row per matched fixture | `match_id` |
| `h2h` | One row per historical H2H match | `h2h_match_id` |

### Main Relationships

```text
teams
  │
  ├────────── match_stats
  │                │
  │                └──────── player_stats
  │
  └────────── match_events

match_stats
  │
  └────────── weather
```

The Gold stage validates the relevant primary-key and foreign-key relationships between these tables.

## 9. Schema Validation

Schema validation and relational validation are treated as separate responsibilities.

### Dataset-level validation

Performed in `03_schema_validate.ipynb`:

- Data types
- Nullable/non-nullable fields
- Allowed values
- Numeric ranges
- Rejected rows and rejection reasons

### Relational validation

Performed in `04_join_transform.ipynb`:

- Primary-key uniqueness
- Foreign-key relationships
- Cross-dataset mappings
- Match/team relationships

This keeps the schema notebook focused on validating individual datasets and the Gold stage responsible for relationships between datasets.

## 10. Source and Data Dictionary

The project uses the six cleaned Silver datasets as the inputs to schema validation:

```text
fixtures
h2h
match_events
match_stats
player_stats
weather
```

The detailed schema definitions are maintained in `03_schema_validate.ipynb` and cover:

- Column
- Data type
- Nullable
- Allowed values/range

The Gold data model and key relationships are documented above.

## 11. Python Source Modules

The `src/` directory contains the reusable Python implementation used during integration.

```text
src/
├── config.py
├── extract.py
├── profile.py
├── clean.py
├── schema.py
└── transform.py
```

The modules correspond to the pipeline responsibilities:

| Module | Responsibility |
|---|---|
| `config.py` | Configuration loading |
| `extract.py` | Data extraction |
| `profile.py` | Data profiling |
| `clean.py` | Data cleaning |
| `schema.py` | Schema validation |
| `transform.py` | Joining and transformation |

The notebooks remain the task-specific development and demonstration artifacts, while the `src/` modules provide reusable pipeline code.

## 12. Main Pipeline

`main.py` is the pipeline entry point.

It orchestrates:

```text
Extraction
    ↓
Profiling & Cleaning
    ↓
Schema Validation
    ↓
Join & Transformation
```

The intended execution order is:

```text
01_extract
    ↓
02_profile_clean
    ↓
03_schema_validate
    ↓
04_join_transform
```

## 13. Testing

Tests are stored in:

```text
tests/
├── test_clean.py
└── test_transform.py
```

The tests cover basic cleaning expectations and Gold transformation/relationship expectations.

Run the tests with:

```bash
pytest
```

The current test suite passes successfully.

## 14. Requirements

Install the Python dependencies with:

```bash
pip install -r requirements.txt
```

The project uses dependencies including:

```text
pandas
numpy
requests
PyYAML
python-dateutil
azure-storage-file-datalake
```

## 15. Running the Project

The notebooks are executed in order:

```text
01_extract.ipynb
        ↓
02_profile_clean.ipynb
        ↓
03_schema_validate.ipynb
        ↓
04_join_transform.ipynb
```

The integrated Python pipeline is orchestrated through:

```bash
python main.py
```

The execution environment must have access to the required APIs, Azure storage, Databricks resources, and Databricks Secrets.

## 16. Git and Development Workflow

GitHub is used for source control.

Databricks is used for notebook development and execution.

Development is performed on the member branch:

```text
AbdulmajeedAlsharafi
```

Changes can be merged into:

```text
main
```

Sensitive credentials and unnecessary generated data should not be committed to GitHub.

## 17. Security

Never commit actual API keys or storage credentials.

Credentials must remain in Databricks Secrets.

Before committing, verify that sensitive values are not present in:

```text
config.yaml
notebooks/
src/
README.md
```
