spark.catalog.setCurrentCatalog("purgo_databricks")

# Databricks PySpark script
# This script automates the migration of files from the Purgo S3 landing folder to the archive folder

# Purpose: Automate the migration of files from the Purgo S3 landing folder to the archive folder
# Author: Giang Nguyen
# Date: 2025-09-30
# Description: This script automates the migration of files from the Purgo S3 landing folder to the archive folder, 
#              including error handling and auditing.

# Import necessary libraries
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from pyspark.sql import SparkSession

# Define the schema for the audit log
audit_schema = StructType([
    StructField("file_name", StringType(), nullable=True),
    StructField("action", StringType(), nullable=True),
    StructField("source_path", StringType(), nullable=True),
    StructField("target_path", StringType(), nullable=True),
    StructField("status", StringType(), nullable=True),
    StructField("reason", StringType(), nullable=True),
    StructField("timestamp", TimestampType(), nullable=True)
])

# Define a function to migrate files
def migrate_files(df):
    """
    Migrate files from the Purgo S3 landing folder to the archive folder.

    Args:
        df (DataFrame): A DataFrame containing the files to migrate.

    Returns:
        None
    """
    # Iterate over each row in the DataFrame
    for row in df.collect():
        # Get the file name, source path, and target path
        file_name = row.file_name
        source_path = row.s3_landing_path
        target_path = row.s3_archive_path

        try:
            # Move the file from the source path to the target path
            dbutils.fs.mv(source_path + "/" + file_name, target_path + "/" + file_name)

            # Create an audit log entry
            audit_log = spark.createDataFrame([(file_name, "MOVE", source_path, target_path, "SUCCESS", None, F.current_timestamp())], audit_schema)

            # Write the audit log entry to the audit log table
            audit_log.write.format("delta").mode("append").saveAsTable("purgo_playground.wrk_s3_file_migration_audit")

        except Exception as e:
            # Create an audit log entry
            audit_log = spark.createDataFrame([(file_name, "MOVE", source_path, target_path, "FAILED", str(e), F.current_timestamp())], audit_schema)

            # Write the audit log entry to the audit log table
            audit_log.write.format("delta").mode("append").saveAsTable("purgo_playground.wrk_s3_file_migration_audit")

# Define a function to get the files to migrate
def get_files_to_migrate():
    """
    Get the files to migrate from the s3_file_process_log table.

    Returns:
        DataFrame: A DataFrame containing the files to migrate.
    """
    # Read the s3_file_process_log table
    df = spark.read.format("delta").table("purgo_playground.s3_file_process_log")

    # Filter the files to migrate
    df = df.filter(df.file_status == "SUCCESS")

    return df

# Initialize SparkSession
spark = SparkSession.builder.getOrCreate()

# Get the files to migrate
files_to_migrate = get_files_to_migrate()

# Migrate the files
migrate_files(files_to_migrate)
