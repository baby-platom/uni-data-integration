from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from app.constants import INPUT_DIR


def _load_csv(
    spark: SparkSession,
    path: Path,
    schema: StructType,
    *,
    header: bool = True,
    infer_schema: bool = False,
) -> DataFrame:
    return spark.read.csv(
        str(path),
        header=header,
        schema=schema,
        inferSchema=infer_schema,
    )


def extract_raw_data(
    spark: SparkSession,
) -> tuple[DataFrame, DataFrame, DataFrame, DataFrame]:
    order_details_schema = StructType(
        [
            StructField("order_details_id", IntegerType(), nullable=False),
            StructField("order_id", IntegerType(), nullable=False),
            StructField("pizza_id", StringType(), nullable=False),
            StructField("quantity", IntegerType(), nullable=False),
        ]
    )

    orders_schema = StructType(
        [
            StructField("order_id", IntegerType(), nullable=False),
            StructField("date", StringType(), nullable=False),
            StructField("time", StringType(), nullable=False),
        ]
    )

    pizza_types_schema = StructType(
        [
            StructField("pizza_type_id", StringType(), nullable=False),
            StructField("name", StringType(), nullable=False),
            StructField("category", StringType(), nullable=False),
            StructField("ingredients", StringType(), nullable=False),
        ]
    )

    pizzas_schema = StructType(
        [
            StructField("pizza_id", StringType(), nullable=False),
            StructField("pizza_type_id", StringType(), nullable=False),
            StructField("size", StringType(), nullable=False),
            StructField("price", StringType(), nullable=False),
        ]
    )

    order_details_df = _load_csv(
        spark,
        INPUT_DIR / "order_details.csv",
        order_details_schema,
    )
    orders_df = _load_csv(
        spark,
        INPUT_DIR / "orders.csv",
        orders_schema,
    )
    pizza_types_df = _load_csv(
        spark,
        INPUT_DIR / "pizza_types.csv",
        pizza_types_schema,
    )
    pizzas_df = _load_csv(
        spark,
        INPUT_DIR / "pizzas.csv",
        pizzas_schema,
    )

    return order_details_df, orders_df, pizza_types_df, pizzas_df
