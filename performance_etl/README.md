# Performance ETL

Performance ETL workspace with a platform bootstrap, a production VALD pipeline,
and a Catapult raw/bronze ingestion path.

Run all commands from `performance_etl`.

## What It Does

- `vald_midnight_full_refresh`: daily at `00:00` Europe/Lisbon
- `vald_intraday_incremental`: every 30 minutes from `06:00` to `23:30`, rebuilding only the current Europe/Lisbon day in silver/gold
- `catapult_daily_full_refresh`: daily at `01:30` Europe/Lisbon, re-extracting the full active Catapult history into raw and replaying it to bronze
- `catapult_intraday_incremental`: every 30 minutes from `06:00` to `23:30`, extracting incremental Catapult activity/performance changes into raw and replaying them to bronze
- `catapult_historical_day_reprocess`: manual replay of already-ingested Catapult raw rows for one Lisbon calendar day into bronze
- all DAGs start unpaused after deploy
- Catapult extraction is visible by account group (`A`, `B`, `U15`, etc.) and endpoint task (`Teams`, `Players`, `Positions`, etc.); activity-dependent data is grouped as `ActivitiesPerformance`
- intraday keeps reference/profile sync live during the day and waits if the midnight full refresh is still running
- midnight now resets VALD extract watermarks to the cutoff, re-extracts the full active VALD history into raw, rebuilds bronze in `etl_staging`, reconciles live bronze from that snapshot, then rebuilds/publishes silver and gold
- live `silver/gold` writers now share a warehouse advisory lock, so intraday skips fast instead of colliding with manual rebuilds or full refresh publishes

## Required Setup

You need:
- Python `3.11+`
- Docker with Compose
- PostgreSQL warehouse
- VALD API credentials

If you plan to run the Catapult raw/bronze pipeline, also add:
- `CATAPULT_BASE_URL`
- the `CATAPULT_*_API_KEY` values referenced by `config/providers/catapult.yml`

Edit `performance_etl/.env` with at least:
- `POSTGRES_HOST`
- `POSTGRES_PORT`
- `POSTGRES_DB`
- `POSTGRES_USER`
- `POSTGRES_PASSWORD`
- `VALD_CLIENT_ID`
- `VALD_CLIENT_SECRET`
- `VALD_TOKEN_URL`
- `VALD_REGION`
- `AIRFLOW_ADMIN_USERNAME`
- `AIRFLOW_ADMIN_PASSWORD`

Important:
- the warehouse DB remains external to this Compose stack
- Airflow metadata now defaults to a local PostgreSQL service inside this Compose stack
- if you need external Airflow metadata, set `AIRFLOW_METADATA_SQL_ALCHEMY_CONN`
- copy `.env.example` to `.env` and fill in your local secrets; do not commit the populated `.env`

Before committing the folder, run:

```bash
python script/check_repo_hygiene.py
```

Optional Airflow metadata DB settings:
- `AIRFLOW_METADATA_POSTGRES_DB`: local metadata database name, default `airflow`
- `AIRFLOW_METADATA_POSTGRES_USER`: local metadata database user, default `airflow`
- `AIRFLOW_METADATA_POSTGRES_PASSWORD`: local metadata database password, default `airflow`
- `AIRFLOW_METADATA_SQL_ALCHEMY_CONN`: full SQLAlchemy connection string override for external metadata DBs

Optional PostgreSQL session safety settings:
- `POSTGRES_APPLICATION_NAME`: base label used in `pg_stat_activity` for ETL sessions
- `POSTGRES_CONNECT_TIMEOUT_SECONDS`: connection timeout in seconds, default `15`
- `POSTGRES_LOCK_TIMEOUT_MS`: fail lock waits after this many milliseconds, default `30000`
- `POSTGRES_STATEMENT_TIMEOUT_MS`: cancel long-running statements after this many milliseconds, default `1800000`
- `POSTGRES_BOOTSTRAP_LOCK_TIMEOUT_MS`: bootstrap-only lock wait timeout, default `600000`
- `POSTGRES_BOOTSTRAP_STATEMENT_TIMEOUT_MS`: bootstrap-only statement timeout, default `3600000`
- `POSTGRES_BOOTSTRAP_LOCK_RETRY_ATTEMPTS`: bootstrap lock-timeout retry attempts, default `6`
- `POSTGRES_BOOTSTRAP_LOCK_RETRY_SLEEP_SECONDS`: seconds to sleep between bootstrap lock retries, default `10`
- `POSTGRES_IDLE_IN_TRANSACTION_SESSION_TIMEOUT_MS`: kill idle open transactions after this many milliseconds, default `300000`
- `POSTGRES_GOLD_STATEMENT_TIMEOUT_MS`: optional override used only for the gold stage
- `POSTGRES_POOL_MIN_CONNECTIONS`: database pool minimum size, default `1`
- `POSTGRES_POOL_MAX_CONNECTIONS`: database pool maximum size, default `12`
- `VALD_REPLAY_CHUNK_SIZE`: global VALD raw->bronze keyset replay chunk size, default `5000`
- `VALD_REPLAY_COMMIT_BATCH_SIZE`: global VALD raw->bronze commit batch size, default `5000`
- `VALD_FORCEFRAME_TRACE_REPLAY_CHUNK_SIZE`: ForceFrame trace replay chunk size override, default `100`
- `VALD_FORCEFRAME_TRACE_REPLAY_COMMIT_BATCH_SIZE`: ForceFrame trace commit batch override, default `25`
- `VALD_DYNAMO_TRACE_REPLAY_CHUNK_SIZE`: DynaMo trace replay chunk size override, default `50`
- `VALD_DYNAMO_TRACE_REPLAY_COMMIT_BATCH_SIZE`: DynaMo trace commit batch override, default `25`
- `CATAPULT_REPLAY_CHUNK_SIZE`: Catapult raw->bronze keyset replay chunk size, default `1000`
- `CATAPULT_SENSOR_DATA_REPLAY_CHUNK_SIZE`: Catapult sensor-data replay chunk size override, default `100`
- `VALD_GOLD_FAMILY_WORKERS`: gold families processed in parallel, default `2`

Set any of the PostgreSQL timeout values to `0` to disable that specific timeout.

## Quick Start

Install:

```bash
pip install -e .[dev]
```

Bootstrap the warehouse:

```bash
bootstrap_database
```

This creates:
- shared schemas and extensions
- active VALD raw/bronze/silver/gold tables
- Catapult raw/bronze tables
- Catapult monthly partitions for `bronze.catapult_stats` and `bronze.catapult_sensor_data`

Validate the runtime setup:

```bash
validate_vald_pipeline --runtime-only --skip-pytest
```

Deploy Airflow safely:

```bash
start_airflow_stack
```

This is the standard deploy command. It:
- waits for active VALD Airflow tasks to finish
- starts the local Airflow metadata PostgreSQL service
- runs `airflow-init`
- starts `airflow-webserver` and `airflow-scheduler`
- keeps both DAGs unpaused

Force a redeploy without waiting:

```bash
start_airflow_stack --skip-wait
```

Airflow UI:

```text
http://localhost:8080
```

## Day-To-Day Commands

Run the full pipeline:

```bash
run_vald_ingestion --runtime-validate
```

Run the Catapult raw/bronze pipeline:

```bash
run_catapult_ingestion
```

Run the Catapult full historical raw/bronze load, including sensor data:

```bash
run_catapult_ingestion --full-refresh --include-sensor-data
```

Run the bounded Catapult review flow for the current pilot accounts:

```bash
run_catapult_review --accounts U15,U16 --days 5
```

Regenerate the Catapult review report from the latest completed raw/replay batches without re-extracting the slice:

```bash
run_catapult_review --accounts U15,U16 --days 5 --audit-only --json-out catapult_review_u15_u16.json
```

Run the safe midnight-style full rebuild:

```bash
run_vald_reset_rebuild --runtime-validate
```

Behavior:
- raw history is preserved as append-only lineage
- VALD extract watermarks are reset to the cutoff so midnight re-captures the full active source set
- bronze is rebuilt in `etl_staging` and reconciled into live bronze by insert/update/delete semantics instead of truncating the live bronze tables wholesale
- silver and gold are rebuilt in `etl_staging`
- live silver and gold are replaced only after the staged rebuild succeeds
- batch logs are not reset automatically
- the staged bronze/silver/gold publish window holds the shared live-write lock for the full rebuild window

Run individual stages:

```bash
run_vald_extract_raw
run_vald_raw_to_bronze
run_vald_bronze_to_silver
run_vald_silver_to_gold
run_vald_resume_pipeline --from-stage silver_to_gold
```

Catapult raw/bronze stages:

```bash
run_catapult_extract_raw
run_catapult_review
run_catapult_raw_to_bronze
run_catapult_ingestion
```

Useful variants:

```bash
run_vald_extract_raw --full-refresh
run_vald_ingestion --modules forcedecks,forceframe
run_vald_raw_to_bronze --skip-reference
run_vald_raw_to_bronze --defer-heavy-tables
run_vald_raw_to_bronze --heavy-tables-only
run_vald_resume_pipeline --from-stage raw_to_bronze --modules forcedecks,forceframe
run_vald_resume_pipeline --from-stage silver_to_gold --runtime-validate
validate_vald_pipeline
run_catapult_extract_raw --accounts A,B
run_catapult_extract_raw --full-refresh
run_catapult_extract_raw --include-sensor-data
run_catapult_review --accounts U15,U16 --days 5
```

Operational note:
- intraday raw capture remains incremental by watermark and may pick up late-arriving historical payloads
- intraday silver/gold writes are scoped to the current Europe/Lisbon day only
- manual `run_vald_resume_pipeline`, `run_vald_bronze_to_silver`, `run_vald_silver_to_gold`, `run_vald_ingestion`, and the midnight rebuild all share the same live-write lock
- if intraday reaches silver/gold while another live writer is active, it exits that stage quickly with a skip instead of waiting

## Cleanup

Clean active VALD tables:

```bash
python script/clean_vald_tables.py --layers all --yes
```

Clean and drop obsolete tables:

```bash
python script/clean_vald_tables.py --layers raw,bronze,silver,gold --drop-obsolete --yes
```

Reset watermarks:

```bash
python script/clean_vald_tables.py --layers all --reset-watermarks --yes
```

Dry-run full cleanup:

```bash
python script/clean_database.py --dry-run
```

## Docker Commands

Status:

```bash
docker compose ps
```

List DAGs:

```bash
docker compose exec airflow-webserver airflow dags list
```

Check DAG import errors:

```bash
docker compose exec airflow-webserver airflow dags list-import-errors
```

Tail scheduler logs:

```bash
docker compose logs -f airflow-scheduler
```

Tail webserver logs:

```bash
docker compose logs -f airflow-webserver
```

Run commands inside the running container:

```bash
docker compose exec airflow-webserver bash -lc "bootstrap_database"
docker compose exec airflow-webserver bash -lc "run_vald_ingestion --runtime-validate"
docker compose exec airflow-webserver bash -lc "validate_vald_pipeline --runtime-only"
```

## Avoid This For Normal Deploys

This restarts containers immediately and can kill running tasks:

```bash
docker compose up --build -d airflow-webserver airflow-scheduler
```

Use `start_airflow_stack` instead.

## Notes

- Historical cutoff: `2024-06-30T23:00:00Z`
- Catapult currently supports raw capture, raw->bronze replay, and bounded review/audit only
- Catapult Airflow orchestration now supports daily full refresh, intraday incremental, and manual historical-day raw->bronze replay
- Catapult review mode now supports a bounded latest-N-day slice with audit output and full sensor coverage
- Catapult provider-native IDs are stored in bronze using text-safe columns because live payloads can return UUID-style strings
- Catapult silver/gold models are still out of scope
- Catapult `entity_tags` raw capture is still skipped because the provider references in this repo do not document a read endpoint for it
- Scheduler containers use `restart: unless-stopped`
- Detailed setup: `docs/runbooks/initial_setup.md`
