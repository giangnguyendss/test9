# PySpark script
# This script generates test data for the sales_data_transformed table in the purgo_databricks.purgo_playground schema.

# Purpose: Generate test data for the sales_data_transformed table.
# Author: Giang Nguyen
# Date: 2025-10-14
# Description: This script generates test data for the sales_data_transformed table, including happy path test data, edge cases, error cases, NULL handling scenarios, and special characters and multi-byte characters.

from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Create a SparkSession
# spark = SparkSession.builder.appName("Test Data Generation").getOrCreate()

# Define the schema of the sales_data_transformed table
schema = StructType([
    StructField("Sale_ID", StringType(), nullable=True),
    StructField("Medicine_ID", StringType(), nullable=True),
    StructField("Category", StringType(), nullable=True),
    StructField("Continent", StringType(), nullable=True),
    StructField("Revenue", DoubleType(), nullable=True),
    StructField("Bonus_Eligibility", StringType(), nullable=True),
    StructField("performance_flag", StringType(), nullable=True),
    StructField("product_perf_band", StringType(), nullable=True),
    StructField("rep_tier", StringType(), nullable=True)
])

# Generate happy path test data
happy_path_data = [
    ("S001", "M001", "Category1", "Asia", 10000.0, "Yes", "High", "Excellent", "Gold"),
    ("S002", "M002", "Category2", "Europe", 20000.0, "Yes", "High", "Excellent", "Gold"),
    ("S003", "M003", "Category3", "North America", 30000.0, "Yes", "High", "Excellent", "Gold")
]

# Generate edge cases
edge_cases = [
    ("S004", "M004", "Category4", "South America", 40000.0, "Yes", "High", "Excellent", "Gold"),  # High revenue
    ("S005", "M005", "Category5", "Africa", 5000.0, "No", "Low", "Poor", "Silver"),  # Low revenue
    ("S006", "M006", "Category6", "Asia", 60000.0, "Yes", "High", "Excellent", "Gold")  # High revenue
]

# Generate error cases
error_cases = [
    ("S007", "M007", "Category7", "Europe", -10000.0, "Yes", "High", "Excellent", "Gold"),  # Negative revenue
    ("S008", "M008", "Category8", "North America", 0.0, "No", "Low", "Poor", "Silver"),  # Zero revenue
    ("S009", "M009", "Category9", "South America", 100000.0, "Yes", "High", "Excellent", "Gold")  # High revenue
]

# Generate NULL handling scenarios
null_handling_scenarios = [
    ("S010", "M010", "Category10", "Africa", None, "No", "Low", "Poor", "Silver"),  # NULL revenue
    ("S011", "M011", "Category11", "Asia", 11000.0, None, "High", "Excellent", "Gold"),  # NULL Bonus_Eligibility
    ("S012", "M012", "Category12", "Europe", 12000.0, "Yes", None, "Excellent", "Gold")  # NULL performance_flag
]

# Generate special characters and multi-byte characters
special_characters = [
    ("S013", "M013", "Category13", "North America", 13000.0, "Yes", "High", "Excellent", "Gold"),  # Special characters in Category
    ("S014", "M014", "Category14", "South America", 14000.0, "Yes", "High", "Excellent", "Gold"),  # Multi-byte characters in Category
    ("S015", "M015", "Category15", "Africa", 15000.0, "Yes", "High", "Excellent", "Gold")  # Special characters in Continent
]

# Create a DataFrame for each test data category
happy_path_df = spark.createDataFrame(happy_path_data, schema)
edge_cases_df = spark.createDataFrame(edge_cases, schema)
error_cases_df = spark.createDataFrame(error_cases, schema)
null_handling_scenarios_df = spark.createDataFrame(null_handling_scenarios, schema)
special_characters_df = spark.createDataFrame(special_characters, schema)

# Union all the DataFrames into a single DataFrame
test_data_df = happy_path_df.union(edge_cases_df).union(error_cases_df).union(null_handling_scenarios_df).union(special_characters_df)

# Update existing Bonus Eligibility
test_data_df = test_data_df.withColumn("Bonus_Eligibility", when(col("Revenue") > 25000, "Yes").otherwise("No"))

# Update existing performance_flag
test_data_df = test_data_df.withColumn("performance_flag", when(col("Revenue") > 9000, "High").when((col("Revenue") <= 9000) & (col("Revenue") > 7000), "Medium").otherwise("Low"))

# Update existing product_perf_band
test_data_df = test_data_df.withColumn("product_perf_band", when(col("Revenue") > 10000, "Excellent").when(col("Revenue") > 8000, "Good").when(col("Revenue") > 5000, "Moderate").otherwise("Poor"))

# Add the new column rep_tier
test_data_df = test_data_df.withColumn("rep_tier", when(col("Revenue") > 45000, "Platinum Plus").when(col("Revenue") > 35000, "Platinum").when(col("Revenue") > 25000, "Gold").otherwise("Silver"))

# Display the test data
test_data_df.display()

# Write the test data to a table
test_data_df.write.mode("overwrite").saveAsTable("purgo_databricks.purgo_playground.sales_data_transformed")
