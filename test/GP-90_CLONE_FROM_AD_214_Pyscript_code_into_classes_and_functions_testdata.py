# PySpark script for generating test data
# Purpose: Generate comprehensive test data for Databricks environment
# Author: Giang Nguyen
# Date: 2025-10-14
# Description: This script generates test data for various scenarios, including happy path, edge cases, error cases, NULL handling, and special characters.

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
# spark = SparkSession.builder.appName("Test Data Generation").getOrCreate()

# Generate happy path test data
happy_path_data = [
    (2022, "product1", "Product 1", "Market Segment 1", 1000, 900, 11.11, 25.0, "High", 1),
    (2022, "product2", "Product 2", "Market Segment 2", 2000, 1800, 11.11, 30.0, "High", 2),
    (2023, "product1", "Product 1", "Market Segment 1", 1200, 1000, 20.0, 25.0, "High", 1),
    (2023, "product2", "Product 2", "Market Segment 2", 2400, 2000, 20.0, 30.0, "High", 2)
]

# Generate edge cases test data
edge_cases_data = [
    (2022, "product1", "Product 1", "Market Segment 1", 0, 0, 0.0, 0.0, "Low", 1),
    (2022, "product2", "Product 2", "Market Segment 2", 1000000, 900000, 11.11, 100.0, "High", 2),
    (2023, "product1", "Product 1", "Market Segment 1", 1200, None, None, 25.0, "High", 1),
    (2023, "product2", "Product 2", "Market Segment 2", 2400, 2000, 20.0, 30.0, "High", 2)
]

# Generate error cases test data
error_cases_data = [
    (2022, "product1", "Product 1", "Market Segment 1", -1000, 900, 11.11, 25.0, "High", 1),
    (2022, "product2", "Product 2", "Market Segment 2", 2000, -1800, 11.11, 30.0, "High", 2),
    (2023, "product1", "Product 1", "Market Segment 1", 1200, 1000, 20.0, -25.0, "High", 1),
    (2023, "product2", "Product 2", "Market Segment 2", 2400, 2000, 20.0, 30.0, "Invalid", 2)
]

# Generate NULL handling test data
null_handling_data = [
    (2022, "product1", "Product 1", "Market Segment 1", 1000, None, None, 25.0, "High", 1),
    (2022, "product2", "Product 2", "Market Segment 2", 2000, 1800, 11.11, None, "High", 2),
    (2023, "product1", "Product 1", "Market Segment 1", 1200, 1000, 20.0, 25.0, None, 1),
    (2023, "product2", "Product 2", "Market Segment 2", 2400, 2000, 20.0, 30.0, "High", None)
]

# Generate special characters test data
special_characters_data = [
    (2022, "product1", "Product 1!", "Market Segment 1@", 1000, 900, 11.11, 25.0, "High", 1),
    (2022, "product2", "Product 2#", "Market Segment 2$", 2000, 1800, 11.11, 30.0, "High", 2),
    (2023, "product1", "Product 1%", "Market Segment 1^", 1200, 1000, 20.0, 25.0, "High", 1),
    (2023, "product2", "Product 2&", "Market Segment 2*", 2400, 2000, 20.0, 30.0, "High", 2)
]

# Create DataFrames for each test data set
happy_path_df = spark.createDataFrame(happy_path_data, sales_kpi_schema)
edge_cases_df = spark.createDataFrame(edge_cases_data, sales_kpi_schema)
error_cases_df = spark.createDataFrame(error_cases_data, sales_kpi_schema)
null_handling_df = spark.createDataFrame(null_handling_data, sales_kpi_schema)
special_characters_df = spark.createDataFrame(special_characters_data, sales_kpi_schema)

# Write the DataFrames to tables
happy_path_df.write.mode("overwrite").saveAsTable("purgo_playground.sales_kpi_happy_path")
edge_cases_df.write.mode("overwrite").saveAsTable("purgo_playground.sales_kpi_edge_cases")
error_cases_df.write.mode("overwrite").saveAsTable("purgo_playground.sales_kpi_error_cases")
null_handling_df.write.mode("overwrite").saveAsTable("purgo_playground.sales_kpi_null_handling")
special_characters_df.write.mode("overwrite").saveAsTable("purgo_playground.sales_kpi_special_characters")

# Stop the SparkSession
# spark.stop()
