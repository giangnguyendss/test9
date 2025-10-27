spark.catalog.setCurrentCatalog("purgo_databricks")

# PySpark script to calculate Total Time Elapsed (TAT) for New Enrollment Review Activity
# Purpose: Calculate TAT for New Enrollment Review Activity and load data into tat_report table
# Author: Giang Nguyen
# Date: 2025-10-27
# Description: This script calculates the Total Time Elapsed (TAT) for New Enrollment Review Activity by subtracting the sr_created_date from the minimum last_modified_date for each case_id.

# Import necessary libraries
from pyspark.sql import functions as F  
from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType  

# Define schema for pat_case table
pat_case_schema = StructType([
    StructField("case_id", StringType(), True),
    StructField("service_request_type", StringType(), True),
    StructField("sr_created_date", DateType(), True)
])

# Define schema for sr_activity table
sr_activity_schema = StructType([
    StructField("case_id", StringType(), True),
    StructField("subject", StringType(), True),
    StructField("status", StringType(), True),
    StructField("last_modified_date", DateType(), True)
])

# Define schema for tat_report table
tat_report_schema = StructType([
    StructField("case_id", StringType(), True),
    StructField("service_request_type", StringType(), True),
    StructField("sr_created_date", DateType(), True),
    StructField("total_time_elapsed", IntegerType(), True)
])

# Load pat_case table
try:
    pat_case_df = spark.read.format("delta").schema(pat_case_schema).load("purgo_databricks.purgo_playground.pat_case")
except Exception as e:
    print(f"Error loading pat_case table: {e}")

# Load sr_activity table
try:
    sr_activity_df = spark.read.format("delta").schema(sr_activity_schema).load("purgo_databricks.purgo_playground.sr_activity")
except Exception as e:
    print(f"Error loading sr_activity table: {e}")

# Filter pat_case table for Patient Foundation service request type
pat_case_filtered_df = pat_case_df.filter(pat_case_df.service_request_type == "Patient Foundation")

# Filter sr_activity table for Perform New Enrollment Review Activity subject and Completed status
sr_activity_filtered_df = sr_activity_df.filter((sr_activity_df.subject == "Perform New Enrollment Review Activity") & (sr_activity_df.status == "Completed"))

# Join pat_case and sr_activity tables on case_id
joined_df = pat_case_filtered_df.join(sr_activity_filtered_df, on="case_id", how="inner")

# Calculate minimum last_modified_date for each case_id
min_last_modified_date_df = joined_df.groupBy("case_id").agg(F.min("last_modified_date").alias("min_last_modified_date"))

# Join pat_case table with minimum last_modified_date
joined_min_last_modified_date_df = pat_case_filtered_df.join(min_last_modified_date_df, on="case_id", how="inner")

# Calculate TAT
tat_df = joined_min_last_modified_date_df.withColumn("total_time_elapsed", F.datediff("min_last_modified_date", "sr_created_date"))

# Adjust TAT for weekends
tat_adjusted_df = tat_df.withColumn("total_time_elapsed", F.when(tat_df.total_time_elapsed < 0, F.lit(None)).otherwise(tat_df.total_time_elapsed))

# Load data into tat_report table
try:
    tat_report_df = tat_adjusted_df.select("case_id", "service_request_type", "sr_created_date", "total_time_elapsed")
    tat_report_df.write.format("delta").mode("overwrite").saveAsTable("purgo_databricks.purgo_playground.tat_report")
except Exception as e:
    print(f"Error loading data into tat_report table: {e}")
