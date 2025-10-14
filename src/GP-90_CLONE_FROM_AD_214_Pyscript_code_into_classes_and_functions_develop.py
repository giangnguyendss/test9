spark.catalog.setCurrentCatalog("purgo_databricks")

# PySpark script for sales data pipeline
# Purpose: Process sales data and calculate KPIs
# Author: Giang Nguyen
# Date: 2025-10-14
# Description: This script processes sales data, joins product and market share information, calculates KPIs, and ranks products by sales per year.

# Import necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, sum as _sum, avg, round, year, expr, row_number, lag
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, LongType, TimestampType

# Define the schema for the sales_kpi table
sales_kpi_schema = StructType([
    StructField("sales_year", IntegerType(), True),
    StructField("sales_product_id", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("market_segment", StringType(), True),
    StructField("total_sales", LongType(), True),
    StructField("prev_year_sales", LongType(), True),
    StructField("yoy_growth_pct", DoubleType(), True),
    StructField("avg_market_share", DoubleType(), True),
    StructField("market_penetration_flag", StringType(), True),
    StructField("sales_rank", IntegerType(), True)
])

# Define a class for data loading and cleaning
class DataLoader:
    def __init__(self, spark):
        self.spark = spark

    def load_data(self, table_name):
        return self.spark.read.table(table_name)

    def clean_data(self, df):
        return df.filter(col("sales_amount").isNotNull()) \
                 .withColumn("sales_year", year(col("sales_date"))) \
                 .withColumnRenamed("product_id", "sales_product_id")

# Define a class for data processing and KPI calculation
class DataProcessor:
    def __init__(self, spark):
        self.spark = spark

    def join_product_info(self, df):
        product_df = self.spark.read.table("purgo_playground.product_data")
        return df.join(product_df, df.sales_product_id == product_df.product_id, "left") \
                 .drop(product_df.product_id)

    def join_market_share_info(self, df):
        market_share_df = self.spark.read.table("purgo_playground.product_marketshare_data")
        return df.join(market_share_df, df.sales_product_id == market_share_df.ms_product_id, "left") \
                 .drop("ms_product_id")

    def calculate_kpi(self, df):
        window_spec = Window.partitionBy("sales_product_id").orderBy("sales_year")
        df = df.groupBy("sales_product_id", "sales_year", "product_name", "market_segment") \
               .agg(
                   _sum("sales_amount").alias("total_sales"),
                   round(avg("market_share_pct"), 2).alias("avg_market_share")
               )

        df = df.withColumn("prev_year_sales", 
                           lag("total_sales", 1).over(window_spec))

        df = df.withColumn("yoy_growth_pct", 
                           round(((col("total_sales") - col("prev_year_sales")) / col("prev_year_sales")) * 100, 2))

        df = df.withColumn("market_penetration_flag", 
                           when(col("avg_market_share") > 25, lit("High"))
                          .when((col("avg_market_share") <= 25) & (col("avg_market_share") >= 10), lit("Medium"))
                          .otherwise(lit("Low")))

        return df

    def rank_products(self, df):
        rank_window = Window.partitionBy("sales_year").orderBy(col("total_sales").desc())
        return df.withColumn("sales_rank", row_number().over(rank_window))

# Define a function to process sales data
def process_sales_data(spark):
    data_loader = DataLoader(spark)
    data_processor = DataProcessor(spark)

    sales_df = data_loader.load_data("purgo_playground.product_sales_data")
    sales_df = data_loader.clean_data(sales_df)

    sales_df = data_processor.join_product_info(sales_df)
    sales_df = data_processor.join_market_share_info(sales_df)

    sales_df = data_processor.calculate_kpi(sales_df)
    sales_df = data_processor.rank_products(sales_df)

    sales_df = sales_df.select(
        "sales_year", "sales_product_id", "product_name", "market_segment",
        "total_sales", "prev_year_sales", "yoy_growth_pct", 
        "avg_market_share", "market_penetration_flag", "sales_rank"
    )

    return sales_df

# Create a SparkSession
spark = SparkSession.builder.appName("Sales Data Pipeline").getOrCreate()

# Process sales data
sales_df = process_sales_data(spark)

# Write the final output to a table
sales_df.write.mode("overwrite").saveAsTable("purgo_playground.sales_kpi")

# Stop the SparkSession
spark.stop()
