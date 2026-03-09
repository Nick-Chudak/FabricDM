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
# META           "id": "901992aa-e62a-49cb-a503-0c0a90093d61"
# META         },
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
import xml.etree.ElementTree as ET
import pandas as pd

# ==============================
# CONFIGURATION
# ==============================
TENANT_ID = "e80da230-9b3d-4201-8ca4-3cc3a231c6d0"
CLIENT_ID = "c5f6c8aa-64b9-4d4f-9419-13b49a9682fa"
CLIENT_SECRET = "Hsx8Q~443YkEGSlhIYdxRBD~FHMXJtuHFEHc1bXh"
FO_ENV = "https://org537f5b28.operations.dynamics.com"
COMPANY = "USMF"
CYCLE = "c1_"
# Lakehouse Paths
# Source Table (Spark/Delta Table)
SOURCE_TABLE_NAME = "silver.c1_project_contract_source"

# Template Paths
TEMPLATE_DIR = "/lakehouse/default/Files/Package Templates/Project Contract Package"
MANIFEST_PATH = f"{TEMPLATE_DIR}/Manifest.xml"
HEADER_PATH = f"{TEMPLATE_DIR}/PackageHeader.xml"

# Output Path for processed zip files
PROCESSED_DIR = "/lakehouse/default/Files/Package Processed"

# Generate a unique Job ID for this specific run
JOB_GROUP_ID = f"{CYCLE}Fabric_Project_Contract" 

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

def get_csv_data_from_table(table_name):
    """
    Reads a Silver Table using Spark and converts it to a CSV string.
    Returns: CSV bytes
    """
    print(f"Reading from Silver Table: {table_name}")
    
    # 1. Read table into Spark DataFrame
    df_spark = spark.table(table_name)
    
    # 2. Convert to Pandas for CSV generation 
    # (Efficient for typical D365 batch sizes. For massive data, use partition writing)
    df_pandas = df_spark.toPandas()
    
    # 3. Drop Fabric system columns if they exist (indexes, watermarks)
    cols_to_drop = [c for c in ["ROW_INDEX", "IngestionDate", "MigrationCycle"] if c in df_pandas.columns]
    if cols_to_drop:
        df_pandas.drop(columns=cols_to_drop, inplace=True)

    print(f"Rows to process: {len(df_pandas)}")

    # 4. Convert to CSV bytes
    csv_buffer = io.StringIO()
    df_pandas.to_csv(csv_buffer, index=False)
    return csv_buffer.getvalue().encode('utf-8')

def update_xml_content(file_path, new_group_id):
    """
    Reads an XML file, updates DefinitionGroupName/Description tags,
    and returns the modified XML string.
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Template file not found: {file_path}")
        
    namespace = "http://schemas.microsoft.com/dynamics/2015/01/DataManagement"
    ET.register_namespace('', namespace)
    
    tree = ET.parse(file_path)
    root = tree.getroot()
    ns = {'dm': namespace}

    # Update Group Name and Description
    for tag in root.findall('.//dm:DefinitionGroupName', ns):
        tag.text = new_group_id
    for tag in root.findall('.//dm:Description', ns):
        tag.text = new_group_id

    out_buffer = io.BytesIO()
    tree.write(out_buffer, encoding='utf-8', xml_declaration=True)
    return out_buffer.getvalue()

def create_and_save_package(csv_bytes, manifest_path, header_path, job_id):
    """
    Bundles the CSV + XMLs, SAVES the zip to the Lakehouse, 
    and returns the zip bytes for upload.
    """
    # 1. Prepare XMLs
    manifest_data = update_xml_content(manifest_path, job_id)
    header_data = update_xml_content(header_path, job_id)

    # 2. Create Zip in Memory
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zip_file:
        # IMPORTANT: Filename must match what is inside your Manifest <FileName> tag.
        # Assuming "Project Contract Source.csv" based on your previous file path.
        zip_file.writestr("Project Contract Source.csv", csv_bytes)
        zip_file.writestr("Manifest.xml", manifest_data)
        zip_file.writestr("PackageHeader.xml", header_data)
    
    final_zip_bytes = zip_buffer.getvalue()

    # 3. SAVE to Lakehouse (Package Processed)
    if not os.path.exists(PROCESSED_DIR):
        os.makedirs(PROCESSED_DIR)
        
    save_path = f"{PROCESSED_DIR}/{job_id}.zip"
    print(f"Archiving package to: {save_path}")
    
    with open(save_path, "wb") as f:
        f.write(final_zip_bytes)
    
    return final_zip_bytes

def upload_package_to_blob(token, zip_bytes):
    """Gets a writable Blob URL and uploads the zip."""
    url = f"{FO_ENV}/data/DataManagementDefinitionGroups/Microsoft.Dynamics.DataEntities.GetAzureWriteUrl"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    payload = {"uniqueFileName": f"Import_{uuid.uuid4()}.zip"}
    
    print("Requesting Blob Upload URL...")
    response = requests.post(url, headers=headers, json=payload)
    response.raise_for_status()
    
    result = response.json()
    blob_url = result.get("BlobUrl") or json.loads(result.get("value", "{}")).get("BlobUrl")
    
    print("Uploading Package to Azure Blob...")
    blob_headers = {"x-ms-blob-type": "BlockBlob"}
    upload_resp = requests.put(blob_url, data=zip_bytes, headers=blob_headers)
    upload_resp.raise_for_status()
    
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
        "legalEntityId": COMPANY
    }
    
    print(f"Triggering Import for Project '{JOB_GROUP_ID}'...")
    response = requests.post(url, headers=headers, json=payload)
    response.raise_for_status()
    
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

# ==============================
# MAIN EXECUTION
# ==============================

try:
    print(f"=== Starting D365 Import (Job ID: {JOB_GROUP_ID}) ===")
    
    # 1. Get Token
    access_token = get_d365_token()
    print("Authentication Successful.")

    # 2. Get Data from Silver Layer
    csv_bytes = get_csv_data_from_table(SOURCE_TABLE_NAME)

    # 3. Create & Archive Package
    print(f"Creating package from templates at: {TEMPLATE_DIR}")
    package_bytes = create_and_save_package(csv_bytes, MANIFEST_PATH, HEADER_PATH, JOB_GROUP_ID)

    # 4. Upload Package to D365
    blob_url = upload_package_to_blob(access_token, package_bytes)

    # 5. Run Import
    execution_id = trigger_import(access_token, blob_url)
    print(f"Job Started. Execution ID: {execution_id}")

    # 6. Monitor
    final_status = monitor_execution(access_token, execution_id)
    print(f"=== Job Finished with Status: {final_status} ===")
    
    if final_status != "Succeeded":
        raise Exception(f"Job failed: {final_status}")

except Exception as e:
    print(f"\nERROR: {str(e)}")
    raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.csv("Files/Package Templates/Project Contract Package/Project contract.csv")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pandas as pd
import requests
import json

# 1. Ensure this matches your uploaded file path
CSV_FILE_PATH = "/lakehouse/default/Files/Customers V3.csv" 

# 2. Define the "Key" column you want to see to identify the row (e.g., CustomerAccount)
# Check your CSV header to ensure this matches exactly (Case Sensitive)
KEY_IDENTIFIER_COL = "CUSTOMERACCOUNT"

def get_job_errors(token, exec_id):
    """Retrieves the specific error log lines."""
    url = f"{FO_ENV}/data/DataManagementDefinitionGroups/Microsoft.Dynamics.DataEntities.GetExecutionErrors"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    payload = {"executionId": exec_id}
    
    response = requests.post(url, headers=headers, json=payload)
    response.raise_for_status()
    
    # Get the value
    result = response.json().get("value", [])
    
    # FIX: If D365 returns a JSON string, parse it into a list
    if isinstance(result, str):
        try:
            return json.loads(result)
        except json.JSONDecodeError:
            print("⚠️ Failed to parse error log string.")
            return []
            
    return result

# ==============================
# MAIN EXECUTION
# ==============================
print(f"\n🔍 FETCHING FAILURE REASONS...")
errors = get_job_errors(access_token, execution_id)
print(errors)
if errors:
    # 2. Load the Original Source File
    try:
        print(f"   Reading source file: {CSV_FILE_PATH}...")
        df_source = pd.read_csv(CSV_FILE_PATH)
        # Create a numeric index column to join on
        df_source['SourceRowIndex'] = df_source.index
    except Exception as e:
        print(f"❌ Failed to read source CSV: {e}")
        df_source = pd.DataFrame()

    # 3. Process Error List
    df_errors = pd.DataFrame(errors)
    
    # Map API column names to friendly names
    df_errors.rename(columns={'RecordId': 'SourceRowIndex', 'ErrorMessage': 'Error Message', 'Field': 'Field'}, inplace=True)
    
    # Convert ID to Integer for merging (D365 returns strings like "0", "1")
    df_errors['SourceRowIndex'] = pd.to_numeric(df_errors['SourceRowIndex'], errors='coerce')

    # 4. MERGE: Join Errors with Source Data
    # This aligns the Error Row # with the Pandas Row #
    if not df_source.empty:
        # Join on the row index
        df_final = pd.merge(df_errors, df_source, on='SourceRowIndex', how='left')
        
        # 5. Clean up the Display
        # Pick columns: Index, Key Identifier (CustomerAccount), Error, Field
        cols_to_show = ['SourceRowIndex', 'Error Message', 'Field']
        
        # Add the Business Key if it exists in the CSV
        if KEY_IDENTIFIER_COL in df_final.columns:
            cols_to_show.insert(1, KEY_IDENTIFIER_COL) # Put Account Number second
        else:
            print(f"⚠️ Warning: Column '{KEY_IDENTIFIER_COL}' not found in CSV headers. Showing all columns.")
            # If key column missing, show first 5 columns of source to help identify
            cols_to_show.extend(df_source.columns[:3].tolist())

        print(f"❌ Found {len(df_final)} failures. Below are the details:")
        display(df_final[cols_to_show])
        
    else:
        # Fallback if source CSV fails to load
        display(df_errors[['SourceRowIndex', 'Error Message', 'Field']])

else:
    print("✅ No errors found (or failed to retrieve logs).")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def recursive_ls(path):
    file_list = []
    
    # Get the items in the current directory
    items = mssparkutils.fs.ls(path)
    
    for item in items:
        if item.isDir:
            # If it's a directory, extend the list by calling the function again
            file_list.extend(recursive_ls(item.path))
        else:
            # If it's a file, append the file object (or just item.path)
            file_list.append(item.path)
            
    return file_list

# Usage
all_files = recursive_ls("Files/")

# Print results
for file_path in all_files:
    print(file_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import requests
import time
import json
import shutil
import os
import uuid
import xml.etree.ElementTree as ET
import pandas as pd
from datetime import datetime

# ==============================
# 1. CONFIGURATION
# ==============================
TENANT_ID = "e80da230-9b3d-4201-8ca4-3cc3a231c6d0"
CLIENT_ID = "c5f6c8aa-64b9-4d4f-9419-13b49a9682fa"
CLIENT_SECRET = "Hsx8Q~443YkEGSlhIYdxRBD~FHMXJtuHFEHc1bXh"
FO_ENV = "https://org537f5b28.operations.dynamics.com"
COMPANY = "USMF"
CYCLE = "c1_"

# Data Settings
SOURCE_TABLE_NAME = "silver.c1_project_contract_source"
ENTITY_NAME = "Project Contract"

# Static Job ID (Reuses the same D365 Data Project)
JOB_GROUP_ID = f"{CYCLE}Fabric_{ENTITY_NAME.replace(' ', '_')}"

# Lakehouse Roots (Standard Linux Mounts)
LAKEHOUSE_ROOT = "/lakehouse/default/Files"
TEMPLATES_ROOT = f"{LAKEHOUSE_ROOT}/Package Templates"
PROCESSED_DIR  = f"{LAKEHOUSE_ROOT}/Package Processed"
TEMP_WORK_DIR  = f"{LAKEHOUSE_ROOT}/Temp/{JOB_GROUP_ID}"

# ==============================
# 2. HELPER FUNCTIONS
# ==============================

def get_actual_template_path():
    """
    Searches the Templates folder for the correct 'Project Contract' folder,
    ignoring case and trailing spaces.
    """
    print(f"🔍 Searching for template in: {TEMPLATES_ROOT}")
    
    if not os.path.exists(TEMPLATES_ROOT):
        raise FileNotFoundError(f"Root templates folder missing: {TEMPLATES_ROOT}")

    candidates = os.listdir(TEMPLATES_ROOT)
    match = next((name for name in candidates if "project contract" in name.lower()), None)
    
    if match:
        full_path = f"{TEMPLATES_ROOT}/{match}"
        print(f"   ✅ Found matching folder: '{match}'")
        return full_path
    else:
        print(f"   ❌ No folder found matching 'Project Contract'. Available: {candidates}")
        raise FileNotFoundError("Template folder not found.")

def get_d365_token():
    url = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/token"
    data = {"grant_type": "client_credentials", "client_id": CLIENT_ID, "client_secret": CLIENT_SECRET, "resource": FO_ENV}
    resp = requests.post(url, data=data)
    resp.raise_for_status()
    return resp.json()["access_token"]

def build_package_on_disk(job_id, template_dir):
    """
    Generates the DMF package in a temp folder (UTF-16 CSV) and Zips it.
    """
    # 1. Setup Clean Temp Folder
    if os.path.exists(TEMP_WORK_DIR):
        shutil.rmtree(TEMP_WORK_DIR)
    os.makedirs(TEMP_WORK_DIR)
    print(f"📂 Created temp work dir: {TEMP_WORK_DIR}")

    # 2. Copy XML Templates
    print(f"   Copying templates...")
    for file in ["Manifest.xml", "PackageHeader.xml"]:
        src = f"{template_dir}/{file}"
        dst = f"{TEMP_WORK_DIR}/{file}"
        if not os.path.exists(src):
             raise FileNotFoundError(f"File missing inside template folder: {file}")
        shutil.copy2(src, dst)

    # 3. Generate CSV from Silver Table (UTF-16)
    print(f"📖 Reading Source Table: {SOURCE_TABLE_NAME}")
    df = spark.table(SOURCE_TABLE_NAME).toPandas()
    
    # Clean System Columns
    sys_cols = ["ROW_INDEX", "IngestionDate", "MigrationCycle", "LEGALENTITYID"]
    df.drop(columns=[c for c in sys_cols if c in df.columns], inplace=True)
    
    # Save CSV with UTF-16 Encoding
    csv_path = f"{TEMP_WORK_DIR}/{ENTITY_NAME}.csv"
    
    print(f"✍️ Writing CSV with UTF-16 encoding...")
    # encoding='utf-16' adds BOM automatically, which D365 typically prefers.
    df.to_csv(csv_path, index=False, encoding='utf-16')
    
    print(f"   CSV Generated ({len(df)} rows)")

    # 4. Update XML Content
    print("   Updating XML manifests...")
    for xml_file in ["Manifest.xml", "PackageHeader.xml"]:
        path = f"{TEMP_WORK_DIR}/{xml_file}"
        ET.register_namespace('', "http://schemas.microsoft.com/dynamics/2015/01/DataManagement")
        tree = ET.parse(path)
        root = tree.getroot()
        ns = {'dm': "http://schemas.microsoft.com/dynamics/2015/01/DataManagement"}
        
        for tag in root.findall('.//dm:DefinitionGroupName', ns): tag.text = job_id
        for tag in root.findall('.//dm:Description', ns): tag.text = job_id
        tree.write(path, encoding='utf-8', xml_declaration=True)

    # 5. Zip the Folder
    print(f"📦 Zipping folder...")
    shutil.make_archive(TEMP_WORK_DIR, 'zip', TEMP_WORK_DIR)
    local_zip_path = f"{TEMP_WORK_DIR}.zip"

    # 6. Save to Archive
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    archive_path = f"{PROCESSED_DIR}/{job_id}_{timestamp}.zip"
    
    if not os.path.exists(PROCESSED_DIR):
        os.makedirs(PROCESSED_DIR)
        
    print(f"💾 Saving Archive to: {archive_path}")
    shutil.copy2(local_zip_path, archive_path)

    return local_zip_path

def execute_d365_import(token, zip_path):
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    
    # 1. Get Blob URL
    print("☁️ Requesting Upload URL...")
    url_req = f"{FO_ENV}/data/DataManagementDefinitionGroups/Microsoft.Dynamics.DataEntities.GetAzureWriteUrl"
    resp = requests.post(url_req, headers=headers, json={"uniqueFileName": f"{uuid.uuid4()}.zip"})
    blob_url = resp.json().get("BlobUrl") or json.loads(resp.json().get("value", "{}")).get("BlobUrl")

    # 2. Upload File
    print("🚀 Uploading Package...")
    with open(zip_path, 'rb') as f:
        requests.put(blob_url, data=f.read(), headers={"x-ms-blob-type": "BlockBlob"}).raise_for_status()

    # 3. Trigger Import
    print(f"▶️ Starting Import Job: {JOB_GROUP_ID}")
    url_import = f"{FO_ENV}/data/DataManagementDefinitionGroups/Microsoft.Dynamics.DataEntities.ImportFromPackage"
    payload = {
        "packageUrl": blob_url,
        "definitionGroupId": JOB_GROUP_ID, 
        "executionId": "",
        "execute": True,
        "overwrite": True, 
        "legalEntityId": COMPANY
    }
    exec_id = requests.post(url_import, headers=headers, json=payload).json().get("value")

    # 4. Monitor
    print(f"👀 Monitoring Execution: {exec_id}")
    url_status = f"{FO_ENV}/data/DataManagementDefinitionGroups/Microsoft.Dynamics.DataEntities.GetExecutionSummaryStatus"
    
    while True:
        status = requests.post(url_status, headers=headers, json={"executionId": exec_id}).json().get("value")
        print(f"   Status: {status}")
        if status in ["Succeeded", "PartiallySucceeded", "Failed", "Canceled"]:
            return status
        time.sleep(15)

# ==============================
# 3. MAIN EXECUTION
# ==============================
try:
    print(f"=== Starting Pipeline: {JOB_GROUP_ID} ===")
    
    # 1. Find Template
    actual_template_dir = get_actual_template_path()

    # 2. Build Package
    zip_file = build_package_on_disk(JOB_GROUP_ID, actual_template_dir)
    
    # 3. Execute Import
    token = get_d365_token()
    final_status = execute_d365_import(token, zip_file)
    
    print(f"=== Pipeline Finished: {final_status} ===")
    
    # Cleanup
    if os.path.exists(zip_file): os.remove(zip_file)
    if os.path.exists(TEMP_WORK_DIR): shutil.rmtree(TEMP_WORK_DIR)

except Exception as e:
    print(f"\n❌ ERROR: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
