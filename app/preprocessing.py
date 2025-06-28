from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    concat_ws,
    lit,
    lower,
    regexp_replace,
    split,
    to_date,
    to_timestamp,
    trim,
    when,
    year,
)
from pyspark.sql.types import DoubleType

EXPECTED_YEAR = 2015
MAX_QUANTITY = 100
ALLOWED_SIZES = ["s", "m", "l", "xl"]
MAX_PRICE = 100.0


def _preprocess_order_details(df: DataFrame) -> DataFrame:
    """Clean and preprocess order_details DataFrame.

    - Drop duplicates
    - Remove rows with nulls
    - Trim whitespace
    - Filter quantity < 1, quantity > `MAX_QUANTITY`
    """
    columns = ["order_details_id", "order_id", "pizza_id", "quantity"]

    df = df.dropDuplicates()
    df = df.dropna(subset=columns)

    for column in columns:
        df = df.withColumn(column, lower(trim(col(column))))

    return df.filter((col("quantity") >= 1) & (col("quantity") <= MAX_QUANTITY))


def _preprocess_orders(df: DataFrame) -> DataFrame:
    """Clean and preprocess orders DataFrame.

    - Drop duplicates
    - Remove rows with nulls
    - Trim whitespace
    - Filter invalid dates/times and year != `EXPECTED_YEAR`
    - Combine date and time into timestamp
    """
    columns = ["order_id", "date", "time"]

    df = df.dropDuplicates()
    df = df.dropna(subset=columns)

    for column in columns:
        df = df.withColumn(column, lower(trim(col(column))))

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
        df = df.withColumn(column, lower(trim(col(column))))

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
        df = df.withColumn(column, lower(trim(col(column))))

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


def preprocess(
    order_details_df: DataFrame,
    orders_df: DataFrame,
    pizza_types_df: DataFrame,
    pizzas_df: DataFrame,
) -> tuple[DataFrame, DataFrame, DataFrame, DataFrame]:
    order_details_clean = _preprocess_order_details(order_details_df)
    orders_clean = _preprocess_orders(orders_df)
    pizza_types_clean = _preprocess_pizza_types(pizza_types_df)
    pizzas_clean = _preprocess_pizzas(pizzas_df)

    return order_details_clean, orders_clean, pizza_types_clean, pizzas_clean
