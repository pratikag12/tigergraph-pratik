from pyspark.sql import SparkSession
from pathlib import Path


def main():
    spark = SparkSession.builder.appName("create-sample-parquet").getOrCreate()

    # Generate 1000 synthetic records
    rows = [
        (i, f"Person_{i}", 20 + (i % 40))  # id, name, age in [20, 59]
        for i in range(1, 1001)
    ]
    columns = ["id", "name", "age"]

    df = spark.createDataFrame(rows, columns)

    out_dir = Path("data")
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = (out_dir / "sample.parquet").absolute()
    print(f"Writing sample parquet to {out_path}")

    df.write.mode("overwrite").parquet(str(out_path))

    connector_options = {
        # Basic connection
        "url": f"http://{conf['tg_ip']}:14240",
        "username": conf["tg_user"],
        "password": conf["tg_pass"],
        "version": "0.2.4",

        # Target graph / loading job
        "graph": conf["tg_graph"],
        "loadingJobName": conf["tg_load_job"],

        # IMPORTANT: use loading job mode instead of query mode
        # and turn off queryType / query so the connector doesn't expect a query.
        "batchSize": "1000",
        "debug": "true",
    }

    (
        df.write.format("tigergraph")
        .options(**connector_options)
        .mode("append")
        .save()
    )

    spark.stop()


if __name__ == "__main__":
    main()
