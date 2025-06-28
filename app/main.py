import logging

from pyspark.sql import SparkSession

from app.analyze import analyze_and_visualize
from app.constants import APP_NAME, OUTPUT_DIR
from app.integration import integrate_and_transform, write_to_parquet
from app.preprocessing import load_and_preprocess


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    spark = SparkSession.builder.appName(APP_NAME).getOrCreate()

    order_details_df, orders_df, pizza_types_df, pizzas_df = load_and_preprocess(spark)

    integrated_df = integrate_and_transform(
        order_details_df,
        orders_df,
        pizza_types_df,
        pizzas_df,
    )
    integrated_df.show(n=10)
    write_to_parquet(integrated_df, OUTPUT_DIR)

    analyze_and_visualize(spark, integrated_df)

    spark.stop()


if __name__ == "__main__":
    main()
