from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    concat_ws,
    lit,
    regexp_replace,
    split,
    to_date,
    to_timestamp,
    trim,
    when,
    year,
)
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from app.constants import INPUT_DIR

EXPECTED_YEAR = 2015
MAX_QUANTITY = 100
ALLOWED_SIZES = ["S", "M", "L", "XL"]
MAX_PRICE = 100.0


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


def _preprocess_order_details(df: DataFrame) -> DataFrame:
    """Clean and preprocess order_details DataFrame.

    - Drop duplicates
    - Remove rows with nulls, quantity < 1, quantity > `MAX_QUANTITY`
    """
    df = df.dropDuplicates()

    return df.filter(
        col("order_details_id").isNotNull()
        & col("order_id").isNotNull()
        & col("pizza_id").isNotNull()
        & col("quantity").isNotNull()
        & (col("quantity") >= 1)
        & (col("quantity") <= MAX_QUANTITY)
    )


def _preprocess_orders(df: DataFrame) -> DataFrame:
    """Clean and preprocess orders DataFrame.

    - Drop duplicates
    - Remove rows with nulls
    - Filter invalid dates/times and year != `EXPECTED_YEAR`
    - Combine date and time into timestamp
    """
    df = df.dropDuplicates()
    df = df.dropna(subset=["order_id", "date", "time"])

    df = df.withColumn("order_date", to_date(col("date"), "yyyy-MM-dd"))
    df = df.withColumn("order_time", to_timestamp(col("time"), "HH:mm:ss"))
    df = df.filter(
        col("order_date").isNotNull()
        & col("order_time").isNotNull()
        & (year(col("order_date")) == EXPECTED_YEAR)
    )

    return df.withColumn(
        "order_timestamp",
        to_timestamp(concat_ws(" ", col("date"), col("time")), "yyyy-MM-dd HH:mm:ss"),
    ).drop("date", "time")


def _preprocess_pizza_types(df: DataFrame) -> DataFrame:
    """Clean and preprocess pizza_types DataFrame.

    - Drop duplicates
    - Remove rows with nulls
    - Trim whitespace
    - Parse ingredients into array
    """
    columns = ["pizza_type_id", "name", "category", "ingredients"]

    df = df.dropDuplicates()
    df = df.dropna(subset=columns)

    for column in columns:
        df = df.withColumn(column, trim(col(column)))

    df = df.withColumn("ingredients", regexp_replace(col("ingredients"), '^"|"$', ""))

    return df.withColumn("ingredient_list", split(col("ingredients"), r",\s*")).drop(
        "ingredients"
    )


def _preprocess_pizzas(df: DataFrame) -> DataFrame:
    """Clean and preprocess pizzas DataFrame.

    - Drop duplicates
    - Remove rows with nulls
    - Trim whitespace
    - Validate size
    - Cast price to `DoubleType` and filter extremes
    """
    columns = ["pizza_id", "pizza_type_id", "size", "price"]

    df = df.dropDuplicates()
    df = df.dropna(subset=columns)

    for column in columns:
        df = df.withColumn(column, trim(col(column)))

    df = df.withColumn("size", trim(col("size")))
    df = df.filter(col("size").isin(ALLOWED_SIZES))

    df = df.withColumn(
        "price",
        when(
            col("price").rlike(r"^\d+(\.\d+)?$"), col("price").cast(DoubleType())
        ).otherwise(lit(None).cast(DoubleType())),
    )

    return df.filter(
        col("price").isNotNull() & (col("price") >= 0.0) & (col("price") <= MAX_PRICE)
    )


def load_and_preprocess(
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

    od_df = _load_csv(spark, INPUT_DIR / "order_details.csv", order_details_schema)
    orders_df = _load_csv(spark, INPUT_DIR / "orders.csv", orders_schema)
    pt_df = _load_csv(spark, INPUT_DIR / "pizza_types.csv", pizza_types_schema)
    pizzas_df = _load_csv(spark, INPUT_DIR / "pizzas.csv", pizzas_schema)

    order_details_clean = _preprocess_order_details(od_df)
    orders_clean = _preprocess_orders(orders_df)
    pizza_types_clean = _preprocess_pizza_types(pt_df)
    pizzas_clean = _preprocess_pizzas(pizzas_df)

    return order_details_clean, orders_clean, pizza_types_clean, pizzas_clean
