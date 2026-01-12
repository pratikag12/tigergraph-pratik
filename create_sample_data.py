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

    spark.stop()


if __name__ == "__main__":
    main()
