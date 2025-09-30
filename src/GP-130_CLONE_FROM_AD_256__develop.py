spark.catalog.setCurrentCatalog("purgo_databricks")

# Import necessary libraries
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

# Define the schema for the patient data
patient_data_schema = StructType([
    StructField("patient_id", StringType(), True),
    StructField("patient_name", StringType(), True),
    StructField("age", StringType(), True),
    StructField("diagnosis", StringType(), True),
    StructField("treatment", StringType(), True),
    StructField("data_loaded_at", TimestampType(), True)
])

# Retrieve S3 folder path from ingest_config_master table
s3_folder_path_df = spark.table("purgo_playground.ingest_config_master").filter(F.col("source_object_name").contains("patient_data")).select("s3_landing_path")

# Check if S3 folder path is retrieved successfully
assert s3_folder_path_df.count() > 0, "S3 folder path not retrieved successfully"

# Define the S3 folder path
s3_folder_path = s3_folder_path_df.collect()[0].s3_landing_path

# Configure Spark to access S3 by retrieving the access_key and secret_key securely from the Databricks secret scope aws_keys
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", dbutils.secrets.get("aws_keys", "access_key"))
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", dbutils.secrets.get("aws_keys", "secret_key"))
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.amazonaws.com") # Add endpoint
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") # Add S3AFileSystem

# Create a DataFrame for the patient data
patient_data_df = spark.readStream.format("csv").option("header", "true").option("inferSchema", "false").schema(patient_data_schema).load(s3_folder_path)

# Write the patient data to the patient_data_auto_loader table
patient_data_df.writeStream.format("delta").option("checkpointLocation", "/mnt/checkpoints/patient_al_cp/").option("mergeSchema", "true").option("path", "/mnt/delta/patient_data_auto_loader/").table("purgo_playground.patient_data_auto_loader") # Add path option
