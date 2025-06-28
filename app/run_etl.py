import logging
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, month, year

from app.constants import OUTPUT_DIR
from app.integration import integrate_and_transform
from app.load import load_raw_data
from app.preprocessing import preprocess


def _write_to_parquet(
    df: DataFrame,
    output_path: Path,
) -> None:
    partition_cols = ["year", "month"]
    df_with_parts = df.withColumn("year", year(col("order_timestamp"))).withColumn(
        "month", month(col("order_timestamp"))
    )

    (
        df_with_parts.write.mode("overwrite")
        .partitionBy(*partition_cols)
        .parquet(str(output_path))
    )


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    spark = SparkSession.builder.appName("PizzaSalesETL").getOrCreate()

    order_details_df, orders_df, pizza_types_df, pizzas_df = load_raw_data(spark)
    order_details_df.describe().show()

    order_details_df, orders_df, pizza_types_df, pizzas_df = preprocess(
        order_details_df,
        orders_df,
        pizza_types_df,
        pizzas_df,
    )

    integrated_df = integrate_and_transform(
        order_details_df,
        orders_df,
        pizza_types_df,
        pizzas_df,
    )
    integrated_df.describe().show()
    integrated_df.show(n=10)

    _write_to_parquet(integrated_df, OUTPUT_DIR)
    spark.stop()


if __name__ == "__main__":
    main()
