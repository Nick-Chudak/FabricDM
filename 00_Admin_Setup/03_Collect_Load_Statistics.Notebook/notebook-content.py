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

execution_id = "DMF_C2_Import_Project_contract-2025-12-30T07:21:25-6838472A73164827AA8579A5CB16DB83"
execution_status = "PartiallySucceeded"
dm_cycle = 2
entity_name = "Project contract"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# OData Entities:
# * DataManagementExecutionJobDetails
# Calls/Methods:
# * GetExecutionErrors
# * GetExecutionSummaryStatus 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import requests
import json
import time
import os
from pyspark.sql.functions import current_timestamp, lit
from pyspark.sql.types import * # Required for StructType, StringType, etc.
from notebookutils import mssparkutils


TENANT_ID = "e80da230-9b3d-4201-8ca4-3cc3a231c6d0"
CLIENT_ID = "c5f6c8aa-64b9-4d4f-9419-13b49a9682fa"
CLIENT_SECRET = "Hsx8Q~443YkEGSlhIYdxRBD~FHMXJtuHFEHc1bXh"
FO_ENV = "https://org537f5b28.operations.dynamics.com"

LOG_BASE_DIR = "/lakehouse/default/Files/Logs"
GOLD_DB = "Lakehouse.gold"

if not os.path.exists(LOG_BASE_DIR):
    os.makedirs(LOG_BASE_DIR, exist_ok=True)

def get_d365_token():
    url = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/token"
    payload = {"grant_type": "client_credentials", "client_id": CLIENT_ID, "client_secret": CLIENT_SECRET, "resource": FO_ENV}
    resp = requests.post(url, data=payload)
    resp.raise_for_status()
    return resp.json()["access_token"]

def get_execution_details(token, job_id):
    endpoint = f"{FO_ENV}/data/DataManagementExecutionJobDetails?$filter=JobId eq '{job_id}'"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    print(f"Querying OData: {endpoint}")
    resp = requests.get(endpoint, headers=headers)
    resp.raise_for_status()
    return resp.json().get("value", [])

def get_execution_errors_raw(token, job_id):
    """Retrieves the raw error string from D365."""
    endpoint = f"{FO_ENV}/data/DataManagementDefinitionGroups/Microsoft.Dynamics.DataEntities.GetExecutionErrors"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    payload = {"executionId": job_id}
    print(f"Requesting Execution Errors for {job_id}...")
    resp = requests.post(endpoint, headers=headers, json=payload)
    if resp.status_code == 200:
        return resp.json().get("value", "") # Returns the raw JSON string
    return None

def get_error_file_url(token, job_id, entity):
    endpoint = f"{FO_ENV}/data/DataManagementDefinitionGroups/Microsoft.Dynamics.DataEntities.GetImportTargetErrorKeysFileUrl"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    payload = {"executionId": job_id, "entityName": entity}
    print(f"Requesting Error Keys URL for {entity}...")
    resp = requests.post(endpoint, headers=headers, json=payload)
    if resp.status_code == 200:
        return resp.json().get("value")
    return None

def download_sas_file(url, folder_path, filename):
    try:
        if not os.path.exists(folder_path): os.makedirs(folder_path, exist_ok=True)
        full_path = os.path.join(folder_path, filename)
        resp = requests.get(url, stream=True)
        resp.raise_for_status()
        with open(full_path, "wb") as f:
            for chunk in resp.iter_content(chunk_size=8192): f.write(chunk)
        return full_path
    except Exception as e:
        print(f"Download failed: {e}")
        return None

def save_text_file(content, folder_path, filename):
    try:
        if not os.path.exists(folder_path): os.makedirs(folder_path, exist_ok=True)
        full_path = os.path.join(folder_path, filename)
        with open(full_path, "w", encoding="utf-8") as f:
            f.write(content)
        return full_path
    except Exception as e:
        print(f"Failed to save text file: {e}")
        return None


# MAIN EXECUTION
print(f"Processing Statistics for: {entity_name}")
print(f"Job ID: {execution_id}")

try:
    token = get_d365_token()
    job_details = get_execution_details(token, execution_id)
    
    if not job_details:
        print("No details found.")
    else:
        print(f"Found {len(job_details)} detail records.")
        gold_rows = []
        
        for record in job_details:
            job_entity = record.get("EntityName")
            target_status = record.get("TargetStatus")
            staging_status = record.get("StagingStatus")
            cnt_staging = int(record.get("StagingRecordsCreatedCount", 0))
            cnt_target = int(record.get("TargetRecordsCreatedCount", 0))
            cnt_updated = int(record.get("TargetRecordsUpdatedCount", 0))
            
            # Logic to calculate errors if D365 returns 0 explicitly but status is Error
            cnt_errors = int(record.get("NumberOfRecordsInError", 0))
            if cnt_errors == 0 and target_status != 'Succeeded':
                cnt_errors = cnt_staging - (cnt_target + cnt_updated)

            print(f"Entity: {job_entity} | Status: {target_status} | Errors: {cnt_errors}")

            # ---------------------------
            # ERROR HANDLING LOGIC
            # ---------------------------
            saved_key_path = None
            saved_errors_path = None
            
            if target_status != "Succeeded":
                run_log_dir = os.path.join(LOG_BASE_DIR, entity_name, execution_id)
                
                # 1. DOWNLOAD KEYS FILE (Prefix: Keys_)
                error_url = get_error_file_url(token, execution_id, job_entity)
                if error_url:
                    print("Downloading Target Error Keys...")
                    # Naming convention: Keys_{ExecutionID}.txt
                    keys_filename = f"Keys_{execution_id}.txt"
                    saved_key_path = download_sas_file(error_url, run_log_dir, keys_filename)

                # 2. DOWNLOAD & PARSE ERROR MESSAGES (Prefix: Errors_)
                raw_error_string = get_execution_errors_raw(token, execution_id)
                if raw_error_string:
                    print("Processing Detailed Error Messages...")
                    # Sanitize control characters for JSON parsing
                    cleaned_string = raw_error_string.replace('\n', '\\n').replace('\r', '\\r')
                    
                    try:
                        error_list = json.loads(cleaned_string)
                        formatted_errors = []
                        
                        for error in error_list:
                            rec_id = error.get('RecordId', 'N/A')
                            msg = error.get('ErrorMessage', '').strip()
                            formatted_errors.append(f"Record {rec_id}: {msg}")
                        
                        # Save to file: Errors_{ExecutionID}.txt
                        if formatted_errors:
                            errors_filename = f"Errors_{execution_id}.txt"
                            file_content = "\n".join(formatted_errors)
                            saved_errors_path = save_text_file(file_content, run_log_dir, errors_filename)
                            print(f"Saved parsed errors to: {saved_errors_path}")
                            
                    except json.JSONDecodeError as e:
                        print(f"Failed to parse error JSON: {e}")
                        # Optionally save raw string if parse fails
                        save_text_file(raw_error_string, run_log_dir, f"RawErrors_{execution_id}.txt")


            # Append to list for Gold Table
            gold_rows.append({
                "JobId": execution_id,
                "EntityName": job_entity,
                "Cycle": dm_cycle,
                "StagingStatus": staging_status,
                "TargetStatus": target_status,
                "StartTime": record.get("StagingStartDateTime"),
                "EndTime": record.get("TargetEndDateTime"),
                "StagingRecordsCreated": cnt_staging,
                "TargetRecordsCreated": cnt_target,
                "TargetRecordsUpdated": cnt_updated,
                "ErrorCount": cnt_errors,
                "ErrorKeysLocalPath": saved_key_path # Points to Keys_ file
            })

        if gold_rows:
            # --- DEFINE SCHEMA EXPLICITLY ---
            schema = StructType([
                StructField("JobId", StringType(), True),
                StructField("EntityName", StringType(), True),
                StructField("Cycle", IntegerType(), True),
                StructField("StagingStatus", StringType(), True),
                StructField("TargetStatus", StringType(), True),
                StructField("StartTime", StringType(), True),
                StructField("EndTime", StringType(), True),
                StructField("StagingRecordsCreated", LongType(), True),
                StructField("TargetRecordsCreated", LongType(), True),
                StructField("TargetRecordsUpdated", LongType(), True),
                StructField("ErrorCount", LongType(), True),
                StructField("ErrorKeysLocalPath", StringType(), True) 
            ])

            # Create DataFrame with Schema
            df_gold = spark.createDataFrame(gold_rows, schema=schema)
            
            # Add Timestamp Column
            df_gold = df_gold.withColumn("IngestionTime", current_timestamp())

            # Save
            clean_table_name = f"{entity_name.replace(' ', '_')}_ExecutionHistory"
            target_table = f"{GOLD_DB}.{clean_table_name}"
            print(f"Saving to Gold Table: {target_table}")
            
            (df_gold.write
                .format("delta")
                .mode("append")
                .saveAsTable(target_table)
            )

            print("Data saved successfully.")

except Exception as e:
    print(f"Error: {e}")
    raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# import requests
# import json
# import time
# import os
# from pyspark.sql.functions import current_timestamp, lit
# from pyspark.sql.types import * # Required for StructType, StringType, etc.
# from notebookutils import mssparkutils

# TENANT_ID = "e80da230-9b3d-4201-8ca4-3cc3a231c6d0"
# CLIENT_ID = "c5f6c8aa-64b9-4d4f-9419-13b49a9682fa"
# CLIENT_SECRET = "Hsx8Q~443YkEGSlhIYdxRBD~FHMXJtuHFEHc1bXh"
# FO_ENV = "https://org537f5b28.operations.dynamics.com"
# LOG_BASE_DIR = "/lakehouse/default/Files/Logs"
# GOLD_DB = "Lakehouse.gold"

# if not os.path.exists(LOG_BASE_DIR):
#     os.makedirs(LOG_BASE_DIR, exist_ok=True)


# def get_d365_token():
#     url = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/token"
#     payload = {"grant_type": "client_credentials", "client_id": CLIENT_ID, "client_secret": CLIENT_SECRET, "resource": FO_ENV}
#     resp = requests.post(url, data=payload)
#     resp.raise_for_status()
#     return resp.json()["access_token"]

# def get_execution_details(token, job_id):
#     endpoint = f"{FO_ENV}/data/DataManagementExecutionJobDetails?$filter=JobId eq '{job_id}'"
#     headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
#     print(f"Querying OData: {endpoint}")
#     resp = requests.get(endpoint, headers=headers)
#     resp.raise_for_status()
#     return resp.json().get("value", [])

# def get_error_messages(token, job_id, entity):
#     endpoint = f"{FO_ENV}/data/DataManagementDefinitionGroups/Microsoft.Dynamics.DataEntities.GetExecutionErrors"
#     headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
#     payload = {"executionId": job_id}
#     print(f"Requesting Error File URL for {entity}...")
#     resp = requests.post(endpoint, headers=headers, json=payload)
#     if resp.status_code == 200:
#         return resp.json().get("value", [])
#     return None


# def get_error_file_url(token, job_id, entity):
#     endpoint = f"{FO_ENV}/data/DataManagementDefinitionGroups/Microsoft.Dynamics.DataEntities.GetImportTargetErrorKeysFileUrl"
#     headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
#     payload = {"executionId": job_id, "entityName": entity}
#     print(f"Requesting Error File URL for {entity}...")
#     resp = requests.post(endpoint, headers=headers, json=payload)
#     if resp.status_code == 200:
#         return resp.json().get("value")
#     return None

# def download_sas_file(url, folder_path, filename):
#     try:
#         if not os.path.exists(folder_path): os.makedirs(folder_path, exist_ok=True)
#         full_path = os.path.join(folder_path, filename)
#         resp = requests.get(url, stream=True)
#         resp.raise_for_status()
#         with open(full_path, "wb") as f:
#             for chunk in resp.iter_content(chunk_size=8192): f.write(chunk)
#         return full_path
#     except Exception as e:
#         print(f"Download failed: {e}")
#         return None


# print(f"Processing Statistics for: {entity_name}")
# print(f"Job ID: {execution_id}")

# try:
#     token = get_d365_token()
#     job_details = get_execution_details(token, execution_id)
    
#     if not job_details:
#         print("No details found.")
#     else:
#         print(f"Found {len(job_details)} detail records.")
#         gold_rows = []
        
#         for record in job_details:
#             job_entity = record.get("EntityName")
#             target_status = record.get("TargetStatus")
#             staging_status = record.get("StagingStatus")
#             cnt_staging = int(record.get("StagingRecordsCreatedCount", 0))
#             cnt_target = int(record.get("TargetRecordsCreatedCount", 0))
#             cnt_updated = int(record.get("TargetRecordsUpdatedCount", 0))
            
#             # Logic to calculate errors if D365 returns 0 explicitly but status is Error
#             cnt_errors = int(record.get("NumberOfRecordsInError", 0))
#             if cnt_errors == 0 and target_status == 'Error':
#                 cnt_errors = cnt_staging - (cnt_target + cnt_updated)

#             print(f"Entity: {job_entity} | Status: {target_status} | Errors: {cnt_errors}")

#             # Download Error Logs if needed
#             saved_key_path = None
#             if target_status == "Error" or cnt_errors > 0:
#                 error_url = get_error_file_url(token, execution_id, job_entity)
#                 if error_url:
#                     run_log_dir = os.path.join(LOG_BASE_DIR, entity_name)
#                     print("Downloading Target Error Keys...")
#                     saved_key_path = download_sas_file(error_url, run_log_dir, execution_id)

#             # Append to list
#             gold_rows.append({
#                 "JobId": execution_id,
#                 "EntityName": job_entity,
#                 "Cycle": dm_cycle,
#                 "StagingStatus": staging_status,
#                 "TargetStatus": target_status,
#                 "StartTime": record.get("StagingStartDateTime"),
#                 "EndTime": record.get("TargetEndDateTime"),
#                 "StagingRecordsCreated": cnt_staging,
#                 "TargetRecordsCreated": cnt_target,
#                 "TargetRecordsUpdated": cnt_updated,
#                 "ErrorCount": cnt_errors,
#                 "ErrorKeysLocalPath": saved_key_path
#             })

#         if gold_rows:
#             # --- FIX: DEFINE SCHEMA EXPLICITLY ---
#             # This prevents [CANNOT_DETERMINE_TYPE] when fields like ErrorKeysLocalPath are None
#             schema = StructType([
#                 StructField("JobId", StringType(), True),
#                 StructField("EntityName", StringType(), True),
#                 StructField("Cycle", IntegerType(), True),
#                 StructField("StagingStatus", StringType(), True),
#                 StructField("TargetStatus", StringType(), True),
#                 StructField("StartTime", StringType(), True),
#                 StructField("EndTime", StringType(), True),
#                 StructField("StagingRecordsCreated", LongType(), True),
#                 StructField("TargetRecordsCreated", LongType(), True),
#                 StructField("TargetRecordsUpdated", LongType(), True),
#                 StructField("ErrorCount", LongType(), True),
#                 StructField("ErrorKeysLocalPath", StringType(), True) # Explicitly String, handles None safely
#             ])

#             # Create DataFrame with Schema
#             df_gold = spark.createDataFrame(gold_rows, schema=schema)
            
#             # Add Timestamp Column
#             df_gold = df_gold.withColumn("IngestionTime", current_timestamp())

#             # Save
#             clean_table_name = f"{entity_name.replace(' ', '_')}_ExecutionHistory"
#             target_table = f"{GOLD_DB}.{clean_table_name}"
#             print(f"Saving to Gold Table: {target_table}")
            
#             (df_gold.write
#                 .format("delta")
#                 .mode("append")
#                 .saveAsTable(target_table)
#             )

#             print("Data saved successfully.")

# except Exception as e:
#     print(f"Error: {e}")
#     raise

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
