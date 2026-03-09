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
import json
import base64
import pandas as pd
from datetime import datetime

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ==============================
# D365 FO CONFIG
# ==============================
TENANT_ID = "e80da230-9b3d-4201-8ca4-3cc3a231c6d0" # Tenant ID identifies Microsoft Entra ID 
CLIENT_ID = "c5f6c8aa-64b9-4d4f-9419-13b49a9682fa" # Client ID identifies App Registration
CLIENT_SECRET = "Hsx8Q~443YkEGSlhIYdxRBD~FHMXJtuHFEHc1bXh" # VALUE - Hsx8Q~443YkEGSlhIYdxRBD~FHMXJtuHFEHc1bXh
#Secret ID a69a4c26-91e4-4e92-ba46-550af3ca1fc4
FO_ENV = "https://org537f5b28.operations.dynamics.com"
RESOURCE = FO_ENV

# DMF PROJECT
DEFINITION_GROUP = "FABRIC_IMPORT"
DATA_ENTITY = "CustCustomerV3Entity"
DATA_ENTITY_NAME = "Customers V3"
# FILE
CSV_FILE_NAME = "GL_Journal.csv"


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Check the connection
def get_token():
    url = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"
    payload = {
        "grant_type": "client_credentials",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "scope": f"{FO_ENV}/.default"
    }
    r = requests.post(url, data=payload)
    print("Status:", r.status_code)
    print("Response:", r.text)
    r.raise_for_status()
    return r.json()["access_token"]

token = get_token()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Check FO OData root
headers = {
    "Authorization": f"Bearer {token}",
    "Accept": "application/json"
}

url = "https://org537f5b28.operations.dynamics.com/data/"
r = requests.get(url, headers=headers)

print("Status:", r.status_code)
print(r.text[:500])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Check DMF access
url = "https://org537f5b28.operations.dynamics.com/data/DataManagementDefinitionGroups"
r = requests.get(url, headers=headers)

print("Status:", r.status_code)
print(r.text[:500])


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

FILE_NAME = "FABRIC_EXPORT-Customers V3.csv"

lakehouse_path = f"/lakehouse/default/Files/{FILE_NAME}"

df = pd.read_csv(lakehouse_path)
df.head()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

local_csv = f"/tmp/{FILE_NAME}"
df.to_csv(local_csv, index=False, encoding="utf-8")

with open(local_csv, "rb") as f:
    encoded_file = base64.b64encode(f.read()).decode("utf-8")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Request headers
HEADERS = {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/json",
    "Accept": "application/json"
}
# Upload file to D365 FO
upload_url = (
    f"{FO_ENV}/data/"
    "DataManagementDefinitionGroups/Microsoft.Dynamics.DataEntities.uploadFile"
)

payload = {
    "fileName": FILE_NAME,
    "fileContent": encoded_file
}

r = requests.post(upload_url, headers=HEADERS, json=payload)
r.raise_for_status()

print("✅ File uploaded to FO")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

attach_url = (
    f"{FO_ENV}/data/"
    "DataManagementDefinitionGroups/Microsoft.Dynamics.DataEntities.attachFileToDefinitionGroup"
)

payload = {
    "definitionGroupId": DEFINITION_GROUP,
    "dataEntityName": DATA_ENTITY,
    "fileName": FILE_NAME
}

r = requests.post(attach_url, headers=HEADERS, json=payload)
r.raise_for_status()

print("✅ File attached to DMF project")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

status_url = (
    f"{FO_ENV}/data/"
    "DataManagementDefinitionGroups/Microsoft.Dynamics.DataEntities.getExecutionSummaryStatus"
)

payload = {
    "executionId": EXECUTION_ID
}

r = requests.get(status_url, headers=HEADERS, json=payload)
r.raise_for_status()

status = r.json()
status


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

errors_url = (
    f"{FO_ENV}/data/"
    "DataManagementDefinitionGroups/Microsoft.Dynamics.DataEntities.getExecutionErrors"
)

payload = {
    "executionId": EXECUTION_ID
}

r = requests.get(errors_url, headers=HEADERS, json=payload)
r.raise_for_status()

errors = r.json()
pd.DataFrame(errors)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

FO_ENV = "org537f5b28.operations.dynamics.com"

CREATE_DG_URL = (
    f"https://{FO_ENV}/api/services/DataManagement/CreateDefinitionGroup"
    f"?company=USMF"
)

payload = {
    "definitionGroupId": "FABRIC_CUSTOMER_IMPORT",
    "description": "Fabric → FO customer import",
    "sourceFormat": "CSV"
}

r = requests.post(
    CREATE_DG_URL,
    headers=HEADERS,
    json=payload
)

print("Status:", r.status_code)
print("Response:", r.text)

r.raise_for_status()

print("✅ Definition group created (or already exists)")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

