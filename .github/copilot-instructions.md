# Copilot Instructions for spark-tigergraph-adls

These instructions guide AI coding agents (GitHub Copilot, etc.) working in this repo. Follow the existing patterns and workflows described here.

## Project Overview
- This repo demonstrates moving data from Azure Data Lake Storage (ADLS) or local Parquet into a TigerGraph instance using Apache Spark.
- Runtime stack:
  - Spark 3.5.1 in a Docker container built from [Dockerfile](Dockerfile).
  - TigerGraph 3.9.x in a separate container from `tigergraph/tigergraph:3.9.3`.
  - Python tooling (pyspark, pyTigerGraph) for schema management and local data generation.
- Core flow:
  1. Start `spark` and `tigergraph` containers via [docker-compose.yml](docker-compose.yml).
  2. Ensure TigerGraph schema and loading job via [create_tg_schema.py](create_tg_schema.py).
  3. Prepare source data (local Parquet via [create_sample_data.py](create_sample_data.py) or ADLS parquet).
  4. Run the Spark job [jobs/adls_to_graph.py](jobs/adls_to_graph.py) with the TigerGraph Spark connector JAR to load data.

## Key Files and Directories
- [Makefile](Makefile): primary developer entry point. Provides targets:
  - `build-image`: build the Spark image.
  - `shell`: open an interactive shell in the Spark container with the workspace mounted.
  - `sample-data`: run `python create_sample_data.py` in the Spark container.
  - `run-job`: run `spark-submit jobs/adls_to_graph.py` with the TigerGraph connector JAR mounted from `./jars`.
  - `up` / `down`: start/stop the `spark` and `tigergraph` containers via docker compose.
  - `tg-schema`: run `python create_tg_schema.py` in the Spark container to (re)create schema + loading job.
- [docker-compose.yml](docker-compose.yml): defines `spark` and `tigergraph` services and mounts:
  - Mounts repo root to `/workspace` in `spark`.
  - Mounts `./jars` to `/opt/spark/jars-extra` for the TigerGraph Spark connector.
- [Dockerfile](Dockerfile): builds the `spark-tg-local` image.
  - Installs `python3`, `pip`, and Python deps from [requirements.txt](requirements.txt).
  - Sets `WORKDIR` to `/workspace` and runs as the `spark` user.
- [configs/tg_config.json](configs/tg_config.json): single source of truth for both source data and TigerGraph connection:
  - `source_type`: `"local"` to read from `local_file_path`, or any other value to use ADLS.
  - ADLS fields: `adls_account`, `adls_key`, `container`, `file_path`.
  - TigerGraph fields: `tg_ip`, `tg_graph`, `tg_load_job`, `tg_user`, `tg_pass`.

## Data and Graph Flow
- Source data:
  - For local testing, set `"source_type": "local"` and ensure `local_file_path` points to a Parquet file (by default `data/sample.parquet`).
  - [create_sample_data.py](create_sample_data.py) creates a DataFrame with `(id, name, age)` and writes it as `data/sample.parquet` using Spark.
- Spark → TigerGraph load ([jobs/adls_to_graph.py](jobs/adls_to_graph.py)):
  - Reads configuration from [configs/tg_config.json](configs/tg_config.json).
  - Uses `get_source_df` to read from either local Parquet or ADLS via `abfss://` and Spark config `fs.azure.account.key.*`.
  - Logs the schema and a sample of rows before writing.
  - Uses the TigerGraph Spark connector (`format("tigergraph")`) with options derived from `tg_config.json`:
    - `url`, `username`, `password`.
    - `version`, `graph`, `loadingJobName`, `query.installed`.
  - Assumes the connector JAR (e.g. `tigergraph-spark-connector-0.2.4.jar`) is present under `./jars` and mounted to `/opt/spark/jars-extra`.
- TigerGraph schema and loading job ([create_tg_schema.py](create_tg_schema.py)):
  - Uses `pyTigerGraph` and [configs/tg_config.json](configs/tg_config.json) for host, graph, job, and credentials.
  - Normalizes `tg_ip` to include an `http://` scheme for `pyTigerGraph`.
  - Drops and recreates the graph and loading job idempotently (safe to rerun):
    - Creates a `Person` vertex `(id INT, name STRING, age INT)` with `primary_id_as_attribute="true"`.
    - Creates the graph with `Person` as its sole vertex and installs the schema.
    - Creates a loading job that maps CSV columns `$0, $1, $2` to `Person` fields and installs the job.

## Typical Workflows (for agents)
- Local development / demo (no ADLS):
  1. Run `make build-image` to build the Spark image.
  2. Run `make up` to start `spark` and `tigergraph` containers.
  3. Ensure [configs/tg_config.json](configs/tg_config.json) has `"source_type": "local"` and matches graph/job names used in create_tg_schema.
  4. Run `make tg-schema` to (re)create the TigerGraph graph and loading job.
  5. Run `make sample-data` to generate `data/sample.parquet`.
  6. Run `make run-job` to execute [jobs/adls_to_graph.py](jobs/adls_to_graph.py) and load data into TigerGraph.
- ADLS-backed flow:
  - Update [configs/tg_config.json](configs/tg_config.json) with real ADLS values (`adls_account`, `adls_key`, `container`, `file_path`) and set `source_type` to a non-`local` value.
  - Keep the same Spark/TigerGraph workflow; only the data source logic changes via `get_source_df`.

## Conventions and Expectations for Changes
- Prefer wiring new configuration through [configs/tg_config.json](configs/tg_config.json) instead of hardcoding values in jobs.
- When introducing new Spark jobs:
  - Follow the pattern in [jobs/adls_to_graph.py](jobs/adls_to_graph.py): small `main()` function, explicit `SparkSession` creation, helper functions for IO (e.g., `get_source_df`).
  - Consider adding new Makefile targets mirroring existing ones (`run-job`, `sample-data`, `tg-schema`) to keep workflows discoverable.
- When extending TigerGraph schema or loading behavior:
  - Keep changes idempotent and use `pyTigerGraph` as in [create_tg_schema.py](create_tg_schema.py): handle `DROP` failures gracefully, then `CREATE` and `INSTALL`.
  - Ensure graph and loading job names stay consistent between `tg_config.json`, `create_tg_schema.py`, and connector options in Spark jobs.
- Container assumptions:
  - Code is executed inside the `spark` container with `/workspace` mounted to the repo root.
  - Path-sensitive logic (like reading `configs/tg_config.json` or `data/*.parquet`) should assume `/workspace` as the working directory.

## How Agents Should Interact With This Repo
- Use the Makefile commands as the primary interface for running pipelines; avoid inventing parallel ad-hoc scripts.
- When adding features, keep the separation of concerns:
  - Schema and loading job definitions live in Python (via `pyTigerGraph`) and GSQL strings (see create_tg_schema).
  - Data movement and transformation live in Spark jobs under [jobs/](jobs/).
  - Runtime wiring and orchestration live in [Makefile](Makefile) and [docker-compose.yml](docker-compose.yml).
- Do not introduce new orchestrators or frameworks (e.g., Airflow) unless explicitly requested; this project is intentionally minimal and container-driven.
