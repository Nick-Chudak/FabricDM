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
legal_entity_id = "USMF"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import requests
import time
import json
import uuid
import os
from notebookutils import mssparkutils 

package_stats = []


# Identity & Environment
TENANT_ID = "e80da230-9b3d-4201-8ca4-3cc3a231c6d0"
CLIENT_ID = "c5f6c8aa-64b9-4d4f-9419-13b49a9682fa"
CLIENT_SECRET = "Hsx8Q~443YkEGSlhIYdxRBD~FHMXJtuHFEHc1bXh"
FO_ENV = "https://org537f5b28.operations.dynamics.com"

# Path to the Zip created in the previous step
zip_file_path = f"/lakehouse/default/Files/Zipped Packages/{entity_name}.zip"

# Construct a Unique Job Name for D365
# Example: "C0_Project_Contract_Import"
JOB_GROUP_ID = f"DMF_C{dm_cycle}_Import_{entity_name.replace(' ', '_')}"


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

    return response.json()["access_token"]

def upload_package_to_blob(token, zip_bytes):
    """Gets a writable Blob URL and uploads the zip."""
    url = f"{FO_ENV}/data/DataManagementDefinitionGroups/Microsoft.Dynamics.DataEntities.GetAzureWriteUrl"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    
    # We use a random UUID for the temp filename in Blob Storage
    payload = {"uniqueFileName": f"Import_{uuid.uuid4()}.zip"}
    
    print("Requesting Blob Upload URL...")
    response = requests.post(url, headers=headers, json=payload)
    response.raise_for_status()
    
    result = response.json()
    # Handle potentially different JSON structures from D365
    blob_url = result.get("BlobUrl") or json.loads(result.get("value", "{}")).get("BlobUrl")
    
    if not blob_url:
        raise Exception("Failed to retrieve Blob URL from D365 response.")

    print("Uploading Package to Azure Blob...")
    blob_headers = {"x-ms-blob-type": "BlockBlob"}
    upload_resp = requests.put(blob_url, data=zip_bytes, headers=blob_headers)
    
    return blob_url

def trigger_import(token, blob_url):
    """Calls ImportFromPackage."""
    url = f"{FO_ENV}/data/DataManagementDefinitionGroups/Microsoft.Dynamics.DataEntities.ImportFromPackage"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    
    payload = {
        "packageUrl": blob_url,
        "definitionGroupId": JOB_GROUP_ID, 
        "executionId": "",
        "execute": True,
        "overwrite": True, 
        "legalEntityId": legal_entity_id
    }
    
    print(f"Triggering Import for Project '{JOB_GROUP_ID}'...")
    response = requests.post(url, headers=headers, json=payload)

    return response.json().get("value")

def monitor_execution(token, execution_id):
    """Polls execution status."""
    url = f"{FO_ENV}/data/DataManagementDefinitionGroups/Microsoft.Dynamics.DataEntities.GetExecutionSummaryStatus"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    payload = {"executionId": execution_id}
    
    print(f"Monitoring Execution ID: {execution_id}")
    while True:
        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status()
        status = response.json().get("value")
        print(f"Current Status: {status}")
        
        if status in ["Succeeded", "PartiallySucceeded", "Failed", "Canceled"]:
            return status
        
        time.sleep(20)


if not os.path.exists(zip_file_path):
    print(f"Error: Zip package not found at {zip_file_path}")

else:
    try:
        print(f"=== Starting D365 Import (Job: {JOB_GROUP_ID}) ===")
        
        # Read the Zip File from the Lakehouse
        print(f"Reading package: {zip_file_path}")
        with open(zip_file_path, "rb") as f:
            zip_bytes = f.read()

        # Get Token
        access_token = get_d365_token()
        print("Authentication Successful.")

        # Upload Package to D365 Blob Storage
        blob_url = upload_package_to_blob(access_token, zip_bytes)

        # Trigger Import
        execution_id = trigger_import(access_token, blob_url)
        print(f"Job Started. Execution ID: {execution_id}")

        # Monitor
        final_status = monitor_execution(access_token, execution_id)
        print(f"=== Job Finished with Status: {final_status} ===")
        
        package_stats.append({
            "Entity": entity_name,
            "ExecutionId": execution_id,
            "Status": final_status
        })
        # if final_status != "Succeeded":
        #     raise Exception(f"Job failed: {final_status}")

    except Exception as e:
        print(f"\nERROR: {str(e)}")

        package_stats.append({
            "Entity": entity_name,
            "ExecutionId": None,
            "Status": "Failed"
        })
        # raise
mssparkutils.notebook.exit(json.dumps(package_stats))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
