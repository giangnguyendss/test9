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

# Define test data for happy path scenarios
happy_path_data = [
    ("12345", "John Doe", "30", "Diabetes", "Medication", "2022-01-01T12:00:00.000+0000"),
    ("67890", "Jane Doe", "40", "Hypertension", "Lifestyle Changes", "2022-01-02T12:00:00.000+0000"),
    ("34567", "Bob Smith", "50", "Asthma", "Inhalers", "2022-01-03T12:00:00.000+0000")
]

# Define test data for edge cases
edge_case_data = [
    ("", "", "", "", "", "2022-01-01T12:00:00.000+0000"),  # empty strings
    (None, None, None, None, None, "2022-01-01T12:00:00.000+0000"),  # null values
    ("12345", "John Doe", "30", "Diabetes", "Medication", None)  # null timestamp
]

# Define test data for error cases
error_case_data = [
    ("12345", "John Doe", "30", "Diabetes", "Medication", " invalid timestamp"),  # invalid timestamp
    ("12345", "John Doe", "30", "Diabetes", "Medication", "2022-02-30T12:00:00.000+0000")  # invalid date
]

# Create DataFrames for each test data set
happy_path_df = spark.createDataFrame(happy_path_data, schema=patient_data_schema)
edge_case_df = spark.createDataFrame(edge_case_data, schema=patient_data_schema)
error_case_df = spark.createDataFrame(error_case_data, schema=patient_data_schema)

# Display the DataFrames
print("Happy Path Data:")
happy_path_df.show()

print("Edge Case Data:")
edge_case_df.show()

print("Error Case Data:")
error_case_df.show()
