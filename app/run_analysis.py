import logging

from pyspark.sql import SparkSession

from app.analyze import analyze_and_visualize
from app.constants import OUTPUT_DIR


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    spark = SparkSession.builder.appName("PizzaSalesAnalysis").getOrCreate()

    df_loaded = spark.read.parquet(str(OUTPUT_DIR))
    analyze_and_visualize(spark, df_loaded)

    spark.stop()


if __name__ == "__main__":
    main()
