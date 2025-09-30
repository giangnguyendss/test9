spark.catalog.setCurrentCatalog("purgo_databricks")

# Import necessary libraries
from pyspark.sql.functions import col, when
from pyspark.sql.types import StringType, DoubleType, IntegerType

# Define the schema for metric_config table
metric_config_schema = [
    ("metric_id", StringType(), True),
    ("active_indicator", StringType(), True),
    ("geographical_average_type", StringType(), True),
    ("metric_data_type", StringType(), True),
    ("metric_description", StringType(), True),
    ("metric_template_name", StringType(), True),
    ("metric_type", StringType(), True),
    ("optional_filters", StringType(), True),
    ("source_table", StringType(), True),
    ("table_type", StringType(), True),
    ("target_table", StringType(), True),
    ("template_parameters", StringType(), True),
    ("bu_filter", StringType(), True),
    ("calling_service_name", StringType(), True),
    ("primary_key", StringType(), True)
]

# Define the schema for metric_master table
metric_master_schema = [
    ("metric_template_name", StringType(), True),
    ("sql_query", StringType(), True),
    ("metric_type", StringType(), True),
    ("view_name", StringType(), True),
    ("dependency", IntegerType(), True)
]

# Load excel data into dataframes
metric_config_df = spark.read.format("csv").option("header", "true").schema(metric_config_schema).load("metric_config.csv")
metric_master_df = spark.read.format("csv").option("header", "true").schema(metric_master_schema).load("metric_master.csv")

# Merge data into metric_config table
metric_config_df.write.format("delta").mode("overwrite").saveAsTable("purgo_playground.metric_config")

# Merge data into metric_master table
metric_master_df.write.format("delta").mode("overwrite").saveAsTable("purgo_playground.metric_master")

# Update metric_config table
metric_config_df.createOrReplaceTempView("temp_excel_data_metric_config")
spark.sql("""
    MERGE INTO purgo_playground.metric_config AS target
    USING temp_excel_data_metric_config AS source
    ON target.metric_id = source.metric_id
    WHEN MATCHED THEN
      UPDATE SET target.active_indicator = source.active_indicator,
                 target.geographical_average_type = source.geographical_average_type,
                 target.metric_data_type = source.metric_data_type,
                 target.metric_description = source.metric_description,
                 target.metric_template_name = source.metric_template_name,
                 target.metric_type = source.metric_type,
                 target.optional_filters = source.optional_filters,
                 target.source_table = source.source_table,
                 target.table_type = source.table_type,
                 target.target_table = source.target_table,
                 target.template_parameters = source.template_parameters,
                 target.bu_filter = source.bu_filter,
                 target.calling_service_name = source.calling_service_name,
                 target.primary_key = source.primary_key
    WHEN NOT MATCHED THEN
      INSERT (metric_id, active_indicator, geographical_average_type, metric_data_type, metric_description, metric_template_name, metric_type, optional_filters, source_table, table_type, target_table, template_parameters, bu_filter, calling_service_name, primary_key)
      VALUES (source.metric_id, source.active_indicator, source.geographical_average_type, source.metric_data_type, source.metric_description, source.metric_template_name, source.metric_type, source.optional_filters, source.source_table, source.table_type, source.target_table, source.template_parameters, source.bu_filter, source.calling_service_name, source.primary_key)
""")

# Update metric_master table
metric_master_df.createOrReplaceTempView("temp_excel_data_metric_master")
spark.sql("""
    MERGE INTO purgo_playground.metric_master AS target
    USING temp_excel_data_metric_master AS source
    ON target.metric_template_name = source.metric_template_name AND target.dependency = source.dependency
    WHEN MATCHED THEN
      UPDATE SET target.sql_query = source.sql_query,
                 target.metric_type = source.metric_type,
                 target.view_name = source.view_name
    WHEN NOT MATCHED THEN
      INSERT (metric_template_name, sql_query, metric_type, view_name, dependency)
      VALUES (source.metric_template_name, source.sql_query, source.metric_type, source.view_name, source.dependency)
""")
