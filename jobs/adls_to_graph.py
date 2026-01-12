from pyspark.sql import SparkSession
import json
import os


def get_source_df(spark: SparkSession, conf: dict):
    """Return a DataFrame from either ADLS or a local parquet file based on config."""

    source_type = conf.get("source_type", "adls").lower()

    if source_type == "local":
        # Local filesystem source (for development / demos)
        local_path = conf.get("local_file_path", "data/sample.parquet")
        # Resolve to workspace-relative absolute path
        abs_path = os.path.abspath(local_path)
        print(f"Reading local parquet file from {abs_path}")
        return spark.read.parquet(abs_path)

    # Default: ADLS source
    print("Reading from ADLS using configured storage account and container")
    spark.conf.set(
        f"fs.azure.account.key.{conf['adls_account']}.dfs.core.windows.net",
        conf["adls_key"],
    )

    adls_path = (
        f"abfss://{conf['container']}@{conf['adls_account']}.dfs.core.windows.net/"
        f"{conf['file_path']}"
    )
    return spark.read.parquet(adls_path)


def main():
    # Initialize Spark with the TigerGraph Connector
    spark = (
        SparkSession.builder
        .appName("ADLS-to-TigerGraph")
        .getOrCreate()
    )

    with open("configs/tg_config.json") as f:
        conf = json.load(f)

    # 1/2. Get source DataFrame (local or ADLS)
    df = get_source_df(spark, conf)

    # Inspect the DataFrame before writing
    print("=== Source DataFrame schema ===")
    df.printSchema()

    print("=== Sample rows from source DataFrame ===")
    df.show(20, truncate=False)  # show up to 20 rows, full columns

    # 3. Write to TigerGraph
    # 3. Write to TigerGraph via loading job mode
    # NOTE: these option keys follow the TigerGraph Spark connector (0.2.x) API.
    # Using "loading.job" tells the connector this is a write (loading job),
    # not a read/query, so it will not require any query.* options.
    connector_options = {
        # TigerGraph connection: use the GSQL/REST proxy port (14240)
        # where the Spark connector expects the /restpp/ddl endpoint.
        "url": f"http://{conf['tg_ip']}:14240",
        "username": conf["tg_user"],
        "password": conf["tg_pass"],

        # TigerGraph server version
        # Using "4.1.0" as a safe baseline for 'latest' images to enable newer features if needed
        "version": "4.1.0",

        # Target graph and loading job
        "graph": conf["tg_graph"],
        "loading.job": conf["tg_load_job"],  # matches CREATE LOADING JOB name
        "loading.filename": "f",             # matches DEFINE FILENAME f in create_tg_schema

        # Match the USING clause in the loading job (defaults are "," and "\n" but we
        # set them explicitly for clarity)
        "loading.separator": ",",
        "loading.eol": "\n",
    }

    print("Writing DataFrame to TigerGraph...") 

    (
        df.write.format("tigergraph")
        .options(**connector_options)
        .mode("append")
        .save()
    )


if __name__ == "__main__":
    main()
