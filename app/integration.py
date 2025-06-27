from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, month, year


def integrate_and_transform(
    df_order_details: DataFrame,
    df_orders: DataFrame,
    df_pizza_types: DataFrame,
    df_pizzas: DataFrame,
) -> DataFrame:
    integrated = (
        df_order_details.join(df_orders, on="order_id", how="inner")
        .join(df_pizzas, on="pizza_id", how="inner")
        .join(df_pizza_types, on="pizza_type_id", how="inner")
    )

    return integrated.withColumn("total_price", col("price") * col("quantity")).select(
        "order_details_id",
        "order_id",
        "order_date",
        "order_time",
        "order_timestamp",
        "pizza_id",
        "pizza_type_id",
        "name",
        "category",
        "ingredient_list",
        "size",
        "price",
        "quantity",
        "total_price",
    )


def write_to_parquet(
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
