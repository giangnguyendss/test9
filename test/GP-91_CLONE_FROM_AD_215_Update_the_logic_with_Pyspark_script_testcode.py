spark.catalog.setCurrentCatalog("purgo_databricks")

# PySpark script
# This script tests the sales data transformations in the purgo_databricks.purgo_playground schema.

# Purpose: Test the sales data transformations.
# Author: Giang Nguyen
# Date: 2025-10-14
# Description: This script tests the sales data transformations, including the update of existing Bonus Eligibility, performance_flag, product_perf_band, and the addition of the new column rep_tier.

from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Create a SparkSession
# spark = SparkSession.builder.appName("Test Sales Data Transformations").getOrCreate()

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

# Generate test data
test_data = [
    ("S001", "M001", "Category1", "Asia", 10000.0, "Yes", "High", "Excellent", "Gold"),
    ("S002", "M002", "Category2", "Europe", 20000.0, "Yes", "High", "Excellent", "Gold"),
    ("S003", "M003", "Category3", "North America", 30000.0, "Yes", "High", "Excellent", "Gold"),
    ("S004", "M004", "Category4", "South America", 40000.0, "Yes", "High", "Excellent", "Gold"),
    ("S005", "M005", "Category5", "Africa", 5000.0, "No", "Low", "Poor", "Silver"),
    ("S006", "M006", "Category6", "Asia", 60000.0, "Yes", "High", "Excellent", "Gold")
]

# Create a DataFrame for the test data
test_df = spark.createDataFrame(test_data, schema)

# Update existing Bonus Eligibility
test_df = test_df.withColumn("Bonus_Eligibility", when(col("Revenue") > 25000, "Yes").otherwise("No"))

# Update existing performance_flag
test_df = test_df.withColumn("performance_flag", when(col("Revenue") > 9000, "High").when((col("Revenue") <= 9000) & (col("Revenue") > 7000), "Medium").otherwise("Low"))

# Update existing product_perf_band
test_df = test_df.withColumn("product_perf_band", when(col("Revenue") > 10000, "Excellent").when(col("Revenue") > 8000, "Good").when(col("Revenue") > 5000, "Moderate").otherwise("Poor"))

# Add the new column rep_tier
test_df = test_df.withColumn("rep_tier", when(col("Revenue") > 45000, "Platinum Plus").when(col("Revenue") > 35000, "Platinum").when(col("Revenue") > 25000, "Gold").otherwise("Silver"))

# Display the test data
test_df.display()

# Write the test data to a table
test_df.write.mode("overwrite").saveAsTable("purgo_databricks.purgo_playground.sales_data_transformed")

# Test the data transformations
assert test_df.filter(col("Revenue") > 25000).count() == 4
assert test_df.filter(col("performance_flag") == "High").count() == 4
assert test_df.filter(col("product_perf_band") == "Excellent").count() == 4
assert test_df.filter(col("rep_tier") == "Platinum Plus").count() == 1

print("All tests passed.")
