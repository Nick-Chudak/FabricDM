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

dm_cycle = 0
files_to_ingest = "Project Contract Source, Customers V3"
entity_name = None

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import os
import re
import json
import pandas as pd
from pyspark.sql.functions import current_timestamp, lit
from pyspark.sql.types import StringType
from notebookutils import mssparkutils # Required for returning values to pipeline

# Define paths
# Note: 'os' needs absolute paths, Spark works with relative paths
ABS_BASE_PATH = "/lakehouse/default/Files/Sources" 
SPARK_BASE_PATH = "Files/Sources"
TARGET_DB = "Lakehouse.bronze"

# Ensure clean input list
file_list = [f.strip() for f in files_to_ingest.split(",") if f.strip()]

# Initialize list to track statistics for pipeline output
ingestion_stats = []

def clean_column_name(col_name):
    """
    Sanitizes column names for Delta Lake compatibility.
    Replaces non-alphanumeric chars with underscores and lowercases.
    """
    clean = re.sub(r'[^a-zA-Z0-9]', '_', col_name.strip())
    return clean.lower()

def read_file_to_spark(file_stem):
    """
    Attempts to read .csv or .xlsx for the given filename stem.
    Returns: (Spark DataFrame, Actual Filename) or (None, None) if not found.
    """
    csv_abs = os.path.join(ABS_BASE_PATH, f"{file_stem}.csv")
    xlsx_abs = os.path.join(ABS_BASE_PATH, f"{file_stem}.xlsx")
    
    # Check for CSV
    if os.path.exists(csv_abs):
        df = spark.read.format("csv") \
            .option("header", "true") \
            .option("inferSchema", "false") \
            .load(f"{SPARK_BASE_PATH}/{file_stem}.csv")
        return df, f"{file_stem}.csv"
            
    # Check for Excel
    elif os.path.exists(xlsx_abs):
        pdf = pd.read_excel(xlsx_abs, dtype=str)
        return spark.createDataFrame(pdf), f"{file_stem}.xlsx"
    
    else:
        print(f"File not found: {file_stem} (checked .csv and .xlsx)")
        return None, None


print(f"Starting ingestion for {len(file_list)} files...")

for file_name in file_list:
    try:
        print(f"\nProcessing: {file_name}...")
        
        # Read Data and get the actual filename (with extension)
        df_raw, actual_filename = read_file_to_spark(file_name)
        
        if df_raw is None:
            continue
            
        # 2. Sanitize Schema (Bronze Hygiene)
        # Rename columns to be Delta-safe (no spaces, all lowercase)
        clean_cols = [clean_column_name(c) for c in df_raw.columns]
        df_clean = df_raw.toDF(*clean_cols)

        # 3. Add Metadata (Creates df_final)
        df_final = df_clean.withColumn("ingestion_timestamp", current_timestamp()) \
                           .withColumn("source_file", lit(actual_filename))

        # 4. Construct Table Name
        # Logic: c{cycle}_{entity}_{file_name} to ensure uniqueness
        clean_fname = re.sub(r'[^a-zA-Z0-9]', '_', file_name.lower())

        if entity_name: # Handle scenario when entity_name is not defined in pipeline
            clean_entity = re.sub(r'[^a-zA-Z0-9]', '_', entity_name.lower()) # Handle scenario when entity_name is not defined in pipeline
            table_suffix = f"{clean_entity}_{clean_fname}"
        else:
            table_suffix = f"{clean_fname}"

        full_table_name = f"c{dm_cycle}_{table_suffix}"
        dest_table = f"{TARGET_DB}.{full_table_name}"

        # 5. Write to Delta
        print(f"Writing to table: {dest_table}")
        (df_final.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(dest_table)
        )
        
        # 6. Capture Stats for Pipeline Return
        row_count = df_final.count()
        print(f"Success: {row_count} rows")
        
        ingestion_stats.append({
            "SourceFile": actual_filename,
            "TargetTable": dest_table,
            "RowCount": row_count,
            "Status": "Success"
        })

    except Exception as e:
        error_msg = str(e)
        print(f"ERROR processing {file_name}: {error_msg}")
        ingestion_stats.append({
            "SourceFile": file_name,
            "TargetTable": "N/A",
            "RowCount": 0,
            "Status": f"Failed: {error_msg}"
        })

print("Ingestion pipeline complete.")

# Return statistics to the Data Factory pipeline as JSON
mssparkutils.notebook.exit(json.dumps(ingestion_stats))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
