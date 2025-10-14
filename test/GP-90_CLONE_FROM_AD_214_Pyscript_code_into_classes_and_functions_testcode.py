spark.catalog.setCurrentCatalog("purgo_databricks")

# PySpark script for testing the sales data pipeline
# Purpose: Test the sales data pipeline for various scenarios
# Author: Giang Nguyen
# Date: 2025-10-14
# Description: This script tests the sales data pipeline for happy path, edge cases, error cases, NULL handling, and special characters.

# Import necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, sum as _sum, avg, round, year, expr, row_number, lag
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, LongType, TimestampType, ArrayType, MapType

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

# Define the schema for the product_sales_data table
product_sales_data_schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("sales_product_id", StringType(), True),
    StructField("sales_amount", LongType(), True),
    StructField("sales_date", TimestampType(), True)
])

# Create a SparkSession
# spark = SparkSession.builder.appName("Test Sales Data Pipeline").getOrCreate()

# Load test data
happy_path_df = spark.read.table("purgo_playground.sales_kpi_happy_path")
edge_cases_df = spark.read.table("purgo_playground.sales_kpi_edge_cases")
error_cases_df = spark.read.table("purgo_playground.sales_kpi_error_cases")
null_handling_df = spark.read.table("purgo_playground.sales_kpi_null_handling")
special_characters_df = spark.read.table("purgo_playground.sales_kpi_special_characters")

# Define a function to test the sales data pipeline
def test_sales_data_pipeline(df):
    # Initial Cleanup
    df = df.filter(col("sales_amount").isNotNull()) \
           .withColumn("sales_year", year(col("sales_date"))) \
           .withColumnRenamed("product_id", "sales_product_id")

    # Join Product Info
    product_df = spark.read.table("purgo_playground.product_data")
    df = df.join(product_df, df.sales_product_id == product_df.product_id, "left") \
           .drop(product_df.product_id)

    # Join Market Share Info
    market_share_df = spark.read.table("purgo_playground.product_marketshare_data")
    df = df.join(market_share_df, df.sales_product_id == market_share_df.ms_product_id, "left") \
           .drop("ms_product_id")

    # KPI Calculation - Total Sales, YoY Growth, Market Penetration
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

    # Rank Products by Sales per Year
    rank_window = Window.partitionBy("sales_year").orderBy(col("total_sales").desc())
    df = df.withColumn("sales_rank", row_number().over(rank_window))

    # Final Output Format
    df = df.select(
        "sales_year", "sales_product_id", "product_name", "market_segment",
        "total_sales", "prev_year_sales", "yoy_growth_pct", 
        "avg_market_share", "market_penetration_flag", "sales_rank"
    )

    return df

# Test the sales data pipeline for happy path
happy_path_result = test_sales_data_pipeline(happy_path_df)
assert happy_path_result.count() == 4
assert happy_path_result.schema == sales_kpi_schema

# Test the sales data pipeline for edge cases
edge_cases_result = test_sales_data_pipeline(edge_cases_df)
assert edge_cases_result.count() == 4
assert edge_cases_result.schema == sales_kpi_schema

# Test the sales data pipeline for error cases
error_cases_result = test_sales_data_pipeline(error_cases_df)
assert error_cases_result.count() == 4
assert error_cases_result.schema == sales_kpi_schema

# Test the sales data pipeline for NULL handling
null_handling_result = test_sales_data_pipeline(null_handling_df)
assert null_handling_result.count() == 4
assert null_handling_result.schema == sales_kpi_schema

# Test the sales data pipeline for special characters
special_characters_result = test_sales_data_pipeline(special_characters_df)
assert special_characters_result.count() == 4
assert special_characters_result.schema == sales_kpi_schema

# Stop the SparkSession
# spark.stop()
