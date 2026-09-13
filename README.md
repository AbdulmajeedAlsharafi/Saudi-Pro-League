# Saudi-Pro-League
ETL pipeline for collecting, cleaning, validating, and transforming Saudi Pro League data.
## Source Definition — Head-to-Head (H2H) Data

**Source name:** API-Football (api-sports.io)

**Endpoint:** `GET https://v3.football.api-sports.io/fixtures/headtohead`

**Authentication:** API key sent via the `x-apisports-key` request header.

**Rate limits (Free plan):**
- 100 requests/day
- An additional undocumented burst limit (~10 requests/minute observed) — requests beyond this return HTTP 429 even when the daily quota isn't exhausted.

**Known Free-plan restrictions:**
- The `last` query parameter is not available on the Free plan.
- The `page` query parameter does not exist for this endpoint — it always returns all matching fixtures in a single response (no pagination needed/possible).
- The `/teams` endpoint (used to look up team rosters) only allows `season` values from 2022 to 2024 on the Free plan. Current-season (2026-27) team roster changes were therefore identified manually via public sources and cross-checked by name search against the API.

**Licence / terms of use:** https://www.api-football.com/documentation-v3

**Scope for this project:** All pairwise head-to-head fixture histories among the 18 teams currently competing in the 2026-27 Saudi Pro League (153 team pairs total).