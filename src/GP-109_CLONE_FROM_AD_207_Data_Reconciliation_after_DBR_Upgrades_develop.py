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
                                   F.when(result[column] == result[column + "_v2"], F.lit("Match"))
                                    .otherwise(F.lit("Mismatch")))
    
    return result

# Load the tables from the catalog
product_plant = spark.table("purgo_databricks.purgo_playground.product_plant")
product_plant_v2 = spark.table("purgo_databricks.purgo_playground.product_plant_v2")

# Compare the data from the two tables
pp_validation_results = compare_data(product_plant, product_plant_v2)

# Drop the pp_validation_results table if it exists
spark.sql("DROP TABLE IF EXISTS purgo_databricks.purgo_playground.pp_validation_results")

# Save the result to a table
pp_validation_results.write.format("delta").saveAsTable("purgo_databricks.purgo_playground.pp_validation_results")

# Show the result
pp_validation_results.show()
