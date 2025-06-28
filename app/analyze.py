import matplotlib.pyplot as plt
import pandas as pd
from pyspark.sql import DataFrame, SparkSession


# ruff: noqa: S608
def _pizzas_sold_distribution(spark: SparkSession, view_name: str) -> DataFrame:
    """Get the distribution of pizzas by total quantity sold."""

    query = f"""
        SELECT
            regexp_replace(
                regexp_replace(name, '^The ', ''),
                ' Pizza$', ''
            ) AS clean_name,
            SUM(quantity) AS total_sold
        FROM {view_name}
        GROUP BY clean_name
        ORDER BY total_sold ASC
    """
    return spark.sql(query)


def _revenue_by_pizza_category(spark: SparkSession, view_name: str) -> DataFrame:
    """Get total revenue grouped by pizza category."""

    query = f"""
        SELECT category, SUM(total_price) AS revenue
        FROM {view_name}
        GROUP BY category
        ORDER BY revenue DESC
    """
    return spark.sql(query)


def _monthly_sales_trend(spark: SparkSession, view_name: str) -> DataFrame:
    """Get a series of monthly revenue."""

    query = f"""
        SELECT
            date_format(order_timestamp, 'MMM') AS month_name,
            SUM(total_price) AS monthly_revenue
        FROM {view_name}
        GROUP BY
            month(order_timestamp),
            date_format(order_timestamp, 'MMM')
        ORDER BY
            month(order_timestamp)
    """
    return spark.sql(query)


def _top_ingredients(
    spark: SparkSession,
    view_name: str,
    limit: int = 10,
) -> DataFrame:
    """Get the most common ingredients across all orders."""

    query = f"""
        SELECT ingredient, COUNT(*) AS freq
        FROM (
            SELECT explode(ingredient_list) AS ingredient
            FROM {view_name}
        ) tmp
        GROUP BY ingredient
        ORDER BY freq DESC
        LIMIT {limit}
    """
    return spark.sql(query)


def _plot_bar(
    df_pandas: pd.DataFrame,
    x: str,
    y: str,
    title: str,
    xlabel: str,
    ylabel: str,
) -> None:
    plt.figure(figsize=(10, 6))
    plt.bar(df_pandas[x], df_pandas[y])

    plt.title(title)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.xticks(rotation=45, ha="right")

    plt.tight_layout()
    plt.show()


def _plot_horizontal_bar(
    df_pandas: pd.DataFrame,
    x: str,
    y: str,
    title: str,
    xlabel: str,
    ylabel: str,
) -> None:
    plt.figure(figsize=(10, 6))
    plt.barh(df_pandas[x], df_pandas[y])

    plt.title(title)
    plt.ylabel(ylabel)
    plt.xlabel(xlabel)
    plt.xlim(left=0)

    plt.tight_layout()
    plt.show()


def _plot_line(
    df_pandas: pd.DataFrame,
    x: str,
    y: str,
    title: str,
    xlabel: str,
    ylabel: str,
) -> None:
    plt.figure(figsize=(10, 6))
    plt.plot(df_pandas[x], df_pandas[y], marker="o")

    plt.title(title)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.xticks(rotation=45, ha="right")

    plt.ylim(bottom=0, top=df_pandas[y].max() * 1.1)

    plt.tight_layout()
    plt.show()


def analyze_and_visualize(spark: SparkSession, df: DataFrame) -> None:
    view_name = "orders"
    df.createOrReplaceTempView(view_name)

    pizza_distribution = _pizzas_sold_distribution(spark, view_name)
    revenue_cat = _revenue_by_pizza_category(spark, view_name)
    sales_trend = _monthly_sales_trend(spark, view_name)
    ingredients_pop = _top_ingredients(spark, view_name)

    _plot_horizontal_bar(
        pizza_distribution.toPandas(),
        x="clean_name",
        y="total_sold",
        title="Pizza Sales Distribution",
        xlabel="Total Quantity Sold",
        ylabel="Pizza Name",
    )
    _plot_bar(
        revenue_cat.toPandas(),
        x="category",
        y="revenue",
        title="Revenue by Pizza Category",
        xlabel="Category",
        ylabel="Revenue ($)",
    )
    _plot_line(
        sales_trend.toPandas(),
        x="month_name",
        y="monthly_revenue",
        title="Monthly Revenue Trend",
        xlabel="Month",
        ylabel="Revenue ($)",
    )
    _plot_bar(
        ingredients_pop.toPandas(),
        x="ingredient",
        y="freq",
        title="Top 10 Ingredients by Frequency",
        xlabel="Ingredient",
        ylabel="Count",
    )
