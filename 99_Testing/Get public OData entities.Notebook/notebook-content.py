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

import requests
import time
import json
import zipfile
import io
import os
import uuid

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

TENANT_ID = "e80da230-9b3d-4201-8ca4-3cc3a231c6d0"
CLIENT_ID = "c5f6c8aa-64b9-4d4f-9419-13b49a9682fa"
CLIENT_SECRET = "Hsx8Q~443YkEGSlhIYdxRBD~FHMXJtuHFEHc1bXh"
FO_ENV = "https://org537f5b28.operations.dynamics.com"
COMPANY = "USMF"

# Generate a unique Job ID for this specific run
JOB_GROUP_ID = "Fabric_Cust_" + str(uuid.uuid4())[:8] 

# ==============================
# HELPER FUNCTIONS
# ==============================

def get_d365_token():
    """Authenticates using Client Credentials and returns the Access Token."""
    token_url = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/token"
    data = {
        "grant_type": "client_credentials",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "resource": FO_ENV
    }
    response = requests.post(token_url, data=data)
    response.raise_for_status()
    return response.json()["access_token"]

access_token = get_d365_token()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ==============================
# ODATA ENTITY EXPLORER
# Lists all public Data Entities available via REST API
# ==============================
import pandas as pd
import requests

def get_public_odata_entities(token, search_filter=None):
    """
    Retrieves the catalog of all public OData entities from D365.
    Optionally filters by name (case-insensitive).
    """
    # The root '/data' endpoint lists all Entity Sets
    url = f"{FO_ENV}/data"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    
    print(f"Querying OData Catalog: {url} ...")
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    
    # D365 returns the list inside the 'value' key
    entities = response.json().get("value", [])
    
    # Load into DataFrame
    df = pd.DataFrame(entities)
    
    # Select useful columns: 'name' is the EntitySet name (used in URLs), 'kind' is usually EntitySet
    if not df.empty:
        df = df[['name', 'kind', 'url']]
        
        # Apply Search Filter if provided
        if search_filter:
            df = df[df['name'].str.contains(search_filter, case=False, na=False)]
    
    return df

# ==============================
# DISPLAY ENTITIES
# ==============================

# 1. Search for DMF or Execution related entities (to solve your status issue)
search_term = "DataManagement" # Change this to "Customer", "Vendor", etc. as needed
print(f"🔍 Searching for entities containing: '{search_term}'")

if 'access_token' in locals():
    df_results = get_public_odata_entities(access_token, search_term)

    if not df_results.empty:
        print(f"Found {len(df_results)} entities:")
        display(df_results)
    else:
        print(f"No public entities found matching '{search_term}'.")
        print("Try searching for 'Execution' or 'History' instead.")
else:
    print("'access_token' is missing. Please run the Authentication cell first.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ==============================
# VIEW ODATA ENTITY CONTENT
# Queries the live data from D365
# ==============================

def view_entity_content(token, entity_name, row_limit=100, columns=None):
    """
    Fetches records from a specific OData entity.
    
    :param entity_name: The Public Collection Name (e.g., 'CustomersV3')
    :param row_limit: How many rows to fetch (uses $top)
    :param columns: List of specific columns to select (uses $select)
    """
    # Build URL with Query Parameters
    params = f"$top={row_limit}"
    
    # Add $select if columns are specified (Optimizes performance)
    if columns:
        select_str = ",".join(columns)
        params += f"&$select={select_str}"
        
    url = f"{FO_ENV}/data/{entity_name}?{params}"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    
    print(f"Querying: {url} ...")
    response = requests.get(url, headers=headers)
    
    if response.status_code != 200:
        print(f"Error {response.status_code}: {response.text}")
        return pd.DataFrame()
    
    # Data is inside 'value'
    data = response.json().get("value", [])
    return pd.DataFrame(data)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for en in df_results["name"]:
    print(en)
    display(view_entity_content(access_token, en, row_limit=10, columns=None))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.createDataFrame(view_entity_content(access_token, "DataManagementExecutionJobDetails", columns=None))

df.write \
  .format("delta") \
  .mode("overwrite") \
  .saveAsTable("ExecutionJobDetails")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
