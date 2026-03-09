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

# CELL ********************

import pandas as pd
import os

# ==============================
# CONFIGURATION
# ==============================
SOURCE_FILE = "/lakehouse/default/Files/FABRIC_EXPORT-Customers V3.csv"
NEW_FILE = "/lakehouse/default/Files/Customers V3.csv"
COLUMN_NAME = "CUSTOMERACCOUNT" # Ensure this matches your CSV header exactly

# ==============================
# EXECUTION
# ==============================
print(f"Reading file: {SOURCE_FILE}...")

# 1. Read the XLSX
# dtype=str ensures we treat Account Numbers as text (prevents 001 becoming 1)
df = pd.read_csv(SOURCE_FILE, dtype=str, encoding='utf-16')

if COLUMN_NAME in df.columns:
    print(f"Modifying column '{COLUMN_NAME}'...")
    
    # 2. Add "-1" to the existing value
    df[COLUMN_NAME] = df[COLUMN_NAME] + "-1"
    
    # 3. Save to new file 5 rows
    df.head(5).to_csv(NEW_FILE, index=False)
    
    print(f"✅ Success! New file saved to: {NEW_FILE}")
    print("   Preview of new values:")
    display(df[[COLUMN_NAME]])
    
else:
    print(f"Error: Column '{COLUMN_NAME}' not found in CSV.")
    print(f"Available columns: {list(df.columns)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pandas as pd
# Load data into pandas DataFrame from "/lakehouse/default/Files/Customers V3.csv"
df = pd.read_csv("/lakehouse/default/Files/Customers V3.csv")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Not supported

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
