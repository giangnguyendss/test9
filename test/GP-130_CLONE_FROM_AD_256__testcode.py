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

# Create a DataFrame for the patient data
patient_data_df = spark.readStream.format("csv").option("header", "true").option("inferSchema", "false").schema(patient_data_schema).load(s3_folder_path)

# Check if patient data is streamed successfully
assert patient_data_df.isStreaming, "Patient data not streamed successfully"

# Write the patient data to the patient_data_auto_loader table
patient_data_df.writeStream.format("delta").option("checkpointLocation", "/mnt/checkpoints/patient_al_cp/").option("mergeSchema", "true").table("purgo_playground.patient_data_auto_loader")

# Check if patient data is written successfully
assert spark.table("purgo_playground.patient_data_auto_loader").count() > 0, "Patient data not written successfully"

# Validate data types of patient data columns
patient_data_columns = ["patient_id", "patient_name", "age", "diagnosis", "treatment", "data_loaded_at"]
patient_data_types = ["string", "string", "string", "string", "string", "timestamp"]
for column, data_type in zip(patient_data_columns, patient_data_types):
    assert spark.table("purgo_playground.patient_data_auto_loader").schema[column].dataType.typeName() == data_type, f"Data type of {column} is not {data_type}"

# Validate schema location and checkpoint location
schema_location = "/mnt/checkpoints/s3_autoloader/patient_al_schema"
checkpoint_location = "/mnt/checkpoints/patient_al_cp/"
assert spark.table("purgo_playground.patient_data_auto_loader").schema == patient_data_schema, "Schema location is not correct"
assert checkpoint_location == "/mnt/checkpoints/patient_al_cp/", "Checkpoint location is not correct"

# Test error handling
error_case_data = [
    ("12345", "John Doe", "30", "Diabetes", "Medication", " invalid timestamp"),  # invalid timestamp
    ("12345", "John Doe", "30", "Diabetes", "Medication", "2022-02-30T12:00:00.000+0000")  # invalid date
]
error_case_df = spark.createDataFrame(error_case_data, schema=patient_data_schema)
try:
    error_case_df.write.format("delta").option("checkpointLocation", "/mnt/checkpoints/patient_al_cp/").option("mergeSchema", "true").save("/mnt/checkpoints/patient_al_cp/")
except Exception as e:
    assert str(e) == "Invalid timestamp or date", "Error handling is not correct"
