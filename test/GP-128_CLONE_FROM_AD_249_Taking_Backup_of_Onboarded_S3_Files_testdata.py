spark.catalog.setCurrentCatalog("purgo_databricks")

# PySpark script
# This script generates test data for the Purgo S3 file migration process

# Purpose: Generate test data for the Purgo S3 file migration process
# Author: Giang Nguyen
# Date: 2025-09-30
# Description: This script generates test data for the Purgo S3 file migration process, including happy path test data, edge cases, error cases, NULL handling scenarios, and special characters and multi-byte characters.

# Import necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

# Define the schema for the test data
schema = StructType([
    StructField("file_name", StringType(), nullable=True),
    StructField("s3_landing_path", StringType(), nullable=True),
    StructField("s3_archive_path", StringType(), nullable=True),
    StructField("file_status", StringType(), nullable=True),
    StructField("file_processed_date", TimestampType(), nullable=True)
])

# Create a SparkSession
# spark = SparkSession.builder.appName("Purgo S3 File Migration Test Data Generation").getOrCreate()

# Define a function to generate test data
def generate_test_data():
    """
    Generate test data for the Purgo S3 file migration process.

    Returns:
        DataFrame: A DataFrame containing the test data.
    """
    # Create a list of test data records
    test_data = [
        # Happy path test data
        ("file1.txt", "s3://agilisium-playground-dev/filestore/purgo/patient_raw", "s3://agilisium-playground-dev/filestore/purgo/patient_raw_archive", "SUCCESS", "2024-03-21T00:00:00.000+0000"),
        ("file2.txt", "s3://agilisium-playground-dev/filestore/purgo/patient_raw", "s3://agilisium-playground-dev/filestore/purgo/patient_raw_archive", "SUCCESS", "2024-03-21T00:00:00.000+0000"),
        # Edge cases
        ("file3.txt", "s3://agilisium-playground-dev/filestore/purgo/patient_raw", "s3://agilisium-playground-dev/filestore/purgo/patient_raw_archive", "FAILED", "2024-03-21T00:00:00.000+0000"),
        ("file4.txt", "s3://agilisium-playground-dev/filestore/purgo/patient_raw", "s3://agilisium-playground-dev/filestore/purgo/patient_raw_archive", "PENDING", "2024-03-21T00:00:00.000+0000"),
        # Error cases
        ("file5.txt", "s3://agilisium-playground-dev/filestore/purgo/patient_raw", "s3://invalid-archive-path", "SUCCESS", "2024-03-21T00:00:00.000+0000"),
        ("file6.txt", "s3://agilisium-playground-dev/filestore/purgo/patient_raw", "s3://agilisium-playground-dev/filestore/purgo/patient_raw_archive", "SUCCESS", "2024-03-21T00:00:00.000+0000"),
        # NULL handling scenarios
        (None, "s3://agilisium-playground-dev/filestore/purgo/patient_raw", "s3://agilisium-playground-dev/filestore/purgo/patient_raw_archive", "SUCCESS", "2024-03-21T00:00:00.000+0000"),
        ("file8.txt", None, "s3://agilisium-playground-dev/filestore/purgo/patient_raw_archive", "SUCCESS", "2024-03-21T00:00:00.000+0000"),
        # Special characters and multi-byte characters
        ("file9.txt", "s3://agilisium-playground-dev/filestore/purgo/patient_raw", "s3://agilisium-playground-dev/filestore/purgo/patient_raw_archive", "SUCCESS", "2024-03-21T00:00:00.000+0000"),
        ("file10.txt", "s3://agilisium-playground-dev/filestore/purgo/patient_raw", "s3://agilisium-playground-dev/filestore/purgo/patient_raw_archive", "SUCCESS", "2024-03-21T00:00:00.000+0000")
    ]

    # Create a DataFrame from the test data
    df = spark.createDataFrame(test_data, schema)

    return df

# Generate the test data
test_data = generate_test_data()

# Display the test data
test_data.show(truncate=False)

# # Stop the SparkSession
# spark.stop()
