# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "b22917a7-11e0-4d1a-ad54-c5fd4d2f9e6e",
# META       "default_lakehouse_name": "Lakehouse",
# META       "default_lakehouse_workspace_id": "27e9c377-c28b-475c-b484-fb4f52ba54d1",
# META       "known_lakehouses": [
# META         {
# META           "id": "b22917a7-11e0-4d1a-ad54-c5fd4d2f9e6e"
# META         }
# META       ]
# META     }
# META   }
# META }

# PARAMETERS CELL ********************

entity_name = "Project contract"
dm_cycle = 0

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import os
import pandas as pd
import csv
import json
from pyspark.sql.functions import col, current_timestamp, lit, monotonically_increasing_id
from notebookutils import mssparkutils # Required for returning values to pipeline

ABS_BASE_PATH = "/lakehouse/default/Files/FO Cleansed" 
SPARK_BASE_PATH = "Files/FO Cleansed"

# Logging
transformation_stats = []

try:
# Dynamic Table Naming
    table_name = entity_name.lower().replace(" ", "_")
    full_table_name = f"c{dm_cycle}_{table_name}"

    # Define Source and Target
    source_table = f"Lakehouse.bronze.{full_table_name}"
    target_table = f"Lakehouse.silver.{full_table_name}"
    target_csv_path = f"{ABS_BASE_PATH}/{entity_name}/{entity_name}.csv"

    print(f"Reading from: {source_table}")
    print(f"Target Table: {target_table}")


    df_bronze = spark.read.table(source_table)

    legal_entity_id = "USMF"

    # Add standard Silver layer metadata columns
    df_silver = (df_bronze.withColumn("LEGALENTITYID", lit(legal_entity_id))
                        .withColumn("ROW_INDEX", monotonically_increasing_id())
                        .withColumn("MigrationCycle", lit(dm_cycle))
                        .withColumn("IngestionDate", current_timestamp())
                        )

    # Reorder Columns (LEGALENTITYID, ROW_INDEX first)
    other_cols = [c for c in df_silver.columns if c not in ["LEGALENTITYID", "ROW_INDEX"]]
    final_cols = ["LEGALENTITYID", "ROW_INDEX"] + other_cols

    df_silver = df_silver.select(final_cols)


    #Additional ETL
    df_silver = df_silver.withColumn("SALESCURRENCY", lit("USD"))

    print(f"Writing to Delta Table: {target_table}...")

    (df_silver.write
        .format("delta")
        .mode("overwrite")
        .partitionBy("LEGALENTITYID") 
        .option("overwriteSchema", "true") 
        .saveAsTable(target_table)
    )

    print(f"Writing CSV to: {target_csv_path}...")

    output_dir = os.path.dirname(target_csv_path)

    # Create the directory if it doesn't exist
    if not os.path.exists(output_dir):
        print(f"Creating missing directory: {output_dir}")
        os.makedirs(output_dir, exist_ok=True)

    # Convert to Pandas (Bring data to driver)
    # WARNING: Ensure your driver has enough RAM for this dataset
    df_pandas = df_silver.toPandas()

    # Write to CSV
    df_pandas.to_csv(
        target_csv_path, 
        index=False,
        encoding='utf-16',       # Adds BOM (Critical for D365)
        lineterminator='\r\n',      # Windows Line Endings
        quoting=csv.QUOTE_MINIMAL   # Excel default (quotes only when needed)
    )

    # Capturing statistics:
    row_count = len(df_pandas)
    transformation_stats.append({
        "Entity": entity_name,
        "SourceTable": source_table,
        "TargetTable": target_table,
        "TargetCSV": target_csv_path,
        "RowCount": row_count,
        "Status": "Success"
    })
except Exception as e:
    error_msg = str(e)
    print(f"ERROR processing {entity_name}: {error_msg}")
    
    transformation_stats.append({
        "Entity": entity_name,
        "TargetTable": "N/A",
        "RowCount": 0,
        "Status": f"Failed: {error_msg}"
    })

mssparkutils.notebook.exit(json.dumps(transformation_stats))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
