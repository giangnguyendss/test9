spark.catalog.setCurrentCatalog("purgo_databricks")

# PySpark script to compute metric columns by executing SQL queries from the metric_master table
# Purpose: Compute metric columns by executing SQL queries from the metric_master table
# Author: Giang Nguyen
# Date: 2025-10-14
# Description: This script reads the metric_config and metric_master tables, executes the SQL queries from the metric_master table in the order specified by the dependency column, and loads the results into the target tables.

# Import required libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from pyspark.sql.types import StringType, IntegerType, StructType, StructField

# Define the schema for the metric_config table
metric_config_schema = StructType([
    StructField("metric_id", StringType(), True),
    StructField("active_indicator", StringType(), True),
    StructField("source_table", StringType(), True),
    StructField("primary_key", StringType(), True),
    StructField("target_table", StringType(), True)
])

# Define the schema for the metric_master table
metric_master_schema = StructType([
    StructField("metric_template_name", StringType(), True),
    StructField("sql_query", StringType(), True),
    StructField("view_name", StringType(), True),
    StructField("dependency", IntegerType(), True)
])

# Create a SparkSession
# spark = SparkSession.builder.appName("Metric Column Calculation").getOrCreate()

# Read the metric_config table
metric_config_df = spark.read.format("delta").option("header", "true").schema(metric_config_schema).load("purgo_databricks.purgo_playground.metric_config")

# Read the metric_master table
metric_master_df = spark.read.format("delta").option("header", "true").schema(metric_master_schema).load("purgo_databricks.purgo_playground.metric_master")

# Define a function to execute the SQL queries from the metric_master table
def execute_sql_queries(df):
    """
    Execute the SQL queries from the metric_master table and load the results into the target tables.

    Args:
        df (DataFrame): The DataFrame containing the metric_master table data.

    Returns:
        None
    """
    # Sort the DataFrame by the dependency column
    df = df.sort(col("dependency"))

    # Iterate over the rows in the DataFrame
    for row in df.collect():
        # Get the SQL query and view name from the current row
        sql_query = row.sql_query
        view_name = row.view_name

        # Execute the SQL query
        try:
            # If the view name is not null, create a view with the query results
            if view_name is not None:
                spark.sql(sql_query).createOrReplaceTempView(view_name)
            # If the view name is null, load the query results directly into the target table
            else:
                # Get the target table name from the metric_config table
                target_table = metric_config_df.filter(col("metric_id") == row.metric_template_name).first().target_table
                spark.sql(sql_query).write.format("delta").mode("append").saveAsTable(target_table)
        except Exception as e:
            # Log any errors that occur during execution
            print(f"Error executing SQL query: {e}")

# Call the function to execute the SQL queries
execute_sql_queries(metric_master_df)

# Test cases
def test_execute_sql_queries():
    # Test case 1: Execute SQL queries with view name
    metric_master_df_test = spark.createDataFrame([
        ("metric1", "SELECT * FROM medical_inquiry_data", "view1", 1),
        ("metric2", "SELECT * FROM mska", "view2", 2)
    ], metric_master_schema)
    execute_sql_queries(metric_master_df_test)
    assert spark.catalog.tableExists("view1")
    assert spark.catalog.tableExists("view2")

    # Test case 2: Execute SQL queries without view name
    metric_master_df_test = spark.createDataFrame([
        ("metric1", "SELECT * FROM medical_inquiry_data", None, 1),
        ("metric2", "SELECT * FROM mska", None, 2)
    ], metric_master_schema)
    execute_sql_queries(metric_master_df_test)
    assert spark.catalog.tableExists("medical_inquiry_data")
    assert spark.catalog.tableExists("mska")

    # Test case 3: Handle errors during execution
    metric_master_df_test = spark.createDataFrame([
        ("metric1", "SELECT * FROM non_existent_table", None, 1)
    ], metric_master_schema)
    execute_sql_queries(metric_master_df_test)
    assert not spark.catalog.tableExists("non_existent_table")

test_execute_sql_queries()

# Stop the SparkSession
# spark.stop()
