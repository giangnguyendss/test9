spark.catalog.setCurrentCatalog("purgo_databricks")

# PySpark script
# This script updates the sales data transformations in the purgo_databricks.purgo_playground schema.

# Purpose: Update the sales data transformations.
# Author: Giang Nguyen
# Date: 2025-10-14
# Description: This script updates the sales data transformations, including the update of existing Bonus Eligibility, performance_flag, product_perf_band, and the addition of the new column rep_tier.

from pyspark.sql import functions as F
from pyspark.sql import types as T

# Read the sales data from the table
sales_data = spark.read.table("purgo_databricks.purgo_playground.sales_data_transformed")

# Update existing Bonus Eligibility
sales_data = sales_data.withColumn("Bonus_Eligibility", F.when(F.col("Revenue") > 25000, "Yes").otherwise("No"))

# Update existing performance_flag
sales_data = sales_data.withColumn("performance_flag", 
                                   F.when(F.col("Revenue") > 9000, "High")
                                    .when((F.col("Revenue") <= 9000) & (F.col("Revenue") > 7000), "Medium")
                                    .otherwise("Low"))

# Update existing product_perf_band
sales_data = sales_data.withColumn("product_perf_band", 
                                   F.when(F.col("Revenue") > 10000, "Excellent")
                                    .when(F.col("Revenue") > 8000, "Good")
                                    .when(F.col("Revenue") > 5000, "Moderate")
                                    .otherwise("Poor"))

# Add the new column rep_tier
sales_data = sales_data.withColumn("rep_tier", 
                                   F.when(F.col("Revenue") > 45000, "Platinum Plus")
                                    .when(F.col("Revenue") > 35000, "Platinum")
                                    .when(F.col("Revenue") > 25000, "Gold")
                                    .otherwise("Silver"))

# Display the updated sales data
sales_data.display()

# Write the updated sales data to a table
sales_data.write.mode("overwrite").saveAsTable("purgo_databricks.purgo_playground.sales_data_transformed")
