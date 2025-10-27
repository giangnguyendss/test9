spark.catalog.setCurrentCatalog("purgo_databricks")

# PySpark script to compare data from two tables product_plant and product_plant_v2
# Purpose: Compare data from two tables and generate a validation column for each comparison
# Author: Giang Nguyen
# Date: 2025-10-27
# Description: This script compares data from two tables product_plant and product_plant_v2, 
#              generates a validation column for each comparison, and stores the result in a table pp_validation_results

# Import necessary libraries
from pyspark.sql import functions as F  
from pyspark.sql.types import StringType, IntegerType, BooleanType  

# Define the function to compare data from two tables
def compare_data(product_plant, product_plant_v2):
    """
    Compare data from two tables product_plant and product_plant_v2.
    
    Args:
        product_plant (DataFrame): The first table to compare.
        product_plant_v2 (DataFrame): The second table to compare.
    
    Returns:
        DataFrame: A table with validation columns for each comparison.
    """
    
    # Get the column names from the first table
    columns = product_plant.columns
    
    # Initialize the result table with the columns from the first table
    result = product_plant
    
    # Iterate over the columns and add the corresponding columns from the second table
    for column in columns:
        # Add the column from the second table with a suffix _v2
        result = result.withColumn(column + "_v2", product_plant_v2[column])
        
        # Add a validation column to compare the values
        result = result.withColumn(column + "_validation", 
                                   F.when(result[column] == result[column + "_v2"], "Match")
                                    .otherwise("Mismatch"))
    
    return result

# Test the function
def test_compare_data():
    # Create a sample data for the tables
    data1 = [("abc", 123, True), ("def", 456, False), ("ghi", 789, True)]
    data2 = [("abc", 123, True), ("def", 456, False), ("jkl", 101, False)]

    # Create DataFrames for the tables
    product_plant = spark.createDataFrame(data1, ["column1", "column2", "column3"])
    product_plant_v2 = spark.createDataFrame(data2, ["column1", "column2", "column3"])

    # Compare the data from the two tables
    pp_validation_results = compare_data(product_plant, product_plant_v2)

    # Check if the result is correct
    expected_result = spark.createDataFrame([
        ("abc", 123, True, "abc", 123, True, "Match", "Match", "Match"),
        ("def", 456, False, "def", 456, False, "Match", "Match", "Match"),
        ("ghi", 789, True, "jkl", 101, False, "Mismatch", "Mismatch", "Mismatch")
    ], ["column1", "column2", "column3", "column1_v2", "column2_v2", "column3_v2", "column1_validation", "column2_validation", "column3_validation"])

    assert pp_validation_results.collect() == expected_result.collect()

# Test error handling
def test_error_handling():
    # Create a sample data for the tables
    data1 = [("abc", 123, True), ("def", 456, False), ("ghi", 789, True)]
    data2 = [("abc", 123, True), ("def", 456, False), ("jkl", 101, False)]

    # Create DataFrames for the tables
    product_plant = spark.createDataFrame(data1, ["column1", "column2", "column3"])
    product_plant_v2 = spark.createDataFrame(data2, ["column1", "column2", "column4"])  # column4 instead of column3

    try:
        # Compare the data from the two tables
        pp_validation_results = compare_data(product_plant, product_plant_v2)
        assert False, "Expected an error"
    except Exception as e:
        assert str(e) == "Cannot resolve column name \"column3\" among (column1, column2, column4)"

# Run the tests
test_compare_data()
test_error_handling()

# Drop the pp_validation_results table if it exists
spark.sql("DROP TABLE IF EXISTS purgo_databricks.purgo_playground.pp_validation_results")

# Create a sample data for the tables
data1 = [("abc", 123, True), ("def", 456, False), ("ghi", 789, True)]
data2 = [("abc", 123, True), ("def", 456, False), ("jkl", 101, False)]

# Create DataFrames for the tables
product_plant = spark.createDataFrame(data1, ["column1", "column2", "column3"])
product_plant_v2 = spark.createDataFrame(data2, ["column1", "column2", "column3"])

# Compare the data from the two tables
pp_validation_results = compare_data(product_plant, product_plant_v2)

# Save the result to a table
pp_validation_results.write.saveAsTable("purgo_databricks.purgo_playground.pp_validation_results")

# Show the result
pp_validation_results.show()
