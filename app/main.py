import logging

from pyspark.sql import SparkSession

from app.analyze import analyze_and_visualize
from app.constants import APP_NAME, OUTPUT_DIR
from app.integration import integrate_and_transform, write_to_parquet
from app.load import load_raw_data
from app.preprocessing import preprocess


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    spark = SparkSession.builder.appName(APP_NAME).getOrCreate()

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
    write_to_parquet(integrated_df, OUTPUT_DIR)

    analyze_and_visualize(spark, integrated_df)

    spark.stop()


if __name__ == "__main__":
    main()
