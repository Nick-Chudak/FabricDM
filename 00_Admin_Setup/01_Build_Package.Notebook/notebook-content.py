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

entity_name = 'Project contract'
dm_cycle = 0
legal_entity_id = "USMF"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import os
import zipfile
import json
import xml.etree.ElementTree as ET
from notebookutils import mssparkutils 


# Base Paths
BASE_PATH = "/lakehouse/default/Files"
PATH_TEMPLATES  = f"{BASE_PATH}/Package Templates/{entity_name}"
# Note: Checks for CSV inside a folder named after the entity
PATH_CSV_SOURCE = f"{BASE_PATH}/FO Cleansed/{entity_name}/{entity_name}.csv" 
PATH_OUTPUT_ZIP = f"{BASE_PATH}/Zipped Packages"

# Dynamic Values for XML Metadata
VAL_DESCRIPTION = f"Cycle {dm_cycle} Fabric Import {entity_name}"
VAL_DEF_GROUP   = f"DMF_C{dm_cycle}_Import_{entity_name.replace(' ', '_')}"

# Output Zip Name (e.g., "0_Project_contract.zip")
ZIP_FULL_PATH = f"{PATH_OUTPUT_ZIP}/{entity_name}.zip"

# Initialize Stats for Pipeline
package_stats = []

def get_modified_xml_content(source_path, tag_updates):
    """
    Reads an XML file, updates specific tags, and returns the content as a string.
    This avoids writing intermediate files to disk.
    """
    if not os.path.exists(source_path):
        raise FileNotFoundError(f"Template not found: {source_path}")

    # Register Namespaces to prevent 'ns0:' prefixes
    ns_url = "http://schemas.microsoft.com/dynamics/2015/01/DataManagement"
    ET.register_namespace('', ns_url)
    ET.register_namespace('i', "http://www.w3.org/2001/XMLSchema-instance")
    
    tree = ET.parse(source_path)
    root = tree.getroot()
    
    # Update Tags
    for tag_name, new_value in tag_updates.items():
        # Search for tag using the namespace
        found_tag = root.find(f"{{{ns_url}}}{tag_name}")
        if found_tag is not None:
            found_tag.text = str(new_value)
        else:
            print(f"Warning: Tag <{tag_name}> not found in {os.path.basename(source_path)}")

    # Return as bytes/string
    return ET.tostring(root, encoding="utf-8", method="xml")


print(f"Starting Package Build: {entity_name} (Cycle: {dm_cycle}) ===")
print(f"CSV Source: {PATH_CSV_SOURCE}")
print(f"Templates:  {PATH_TEMPLATES}")

try:
    # --- A. Validation ---
    # Check CSV
    if not os.path.exists(PATH_CSV_SOURCE):
        raise FileNotFoundError(f"Source CSV missing at: {PATH_CSV_SOURCE}")
    
    # Check Templates
    template_manifest = f"{PATH_TEMPLATES}/Manifest.xml"
    template_header = f"{PATH_TEMPLATES}/PackageHeader.xml"
    
    if not os.path.exists(template_manifest) or not os.path.exists(template_header):
        raise FileNotFoundError(f"Missing XML templates in: {PATH_TEMPLATES}")

    # Ensure Output Directory
    os.makedirs(PATH_OUTPUT_ZIP, exist_ok=True)

    # Generate XML Content (In-Memory) ---
    print("Processing XML Templates...")
    
    # Update PackageHeader.xml
    header_content = get_modified_xml_content(
        template_header, 
        {"Description": VAL_DESCRIPTION}
    )

    # Update Manifest.xml
    manifest_content = get_modified_xml_content(
        template_manifest, 
        {
            "Description": VAL_DESCRIPTION,
            "DefinitionGroupName": VAL_DEF_GROUP
        }
    )

    # Build Zip Package ---
    print(f"Creating Zip: {ZIP_FULL_PATH}")
    
    with zipfile.ZipFile(ZIP_FULL_PATH, 'w', zipfile.ZIP_DEFLATED) as zf:
        # Write Modified XMLs (from memory variables)
        zf.writestr("PackageHeader.xml", header_content)
        zf.writestr("Manifest.xml", manifest_content)
        
        # Write CSV (from disk)
        # arcname="{entity_name}.csv" places it at the root of the zip
        zf.write(PATH_CSV_SOURCE, arcname=f"{entity_name}.csv")

    # Collect Statistics ---
    file_size_kb = os.path.getsize(ZIP_FULL_PATH) / 1024
    print(f"Success! Package created ({file_size_kb:.2f} KB)")

    package_stats.append({
        "Entity": entity_name,
        "OutputPath": ZIP_FULL_PATH,
        "SizeKB": round(file_size_kb, 2),
        "Status": "Success"
    })

except Exception as e:
    error_msg = str(e)
    print(f"ERROR building package for {entity_name}: {error_msg}")
    
    package_stats.append({
        "Entity": entity_name,
        "PackageName": "N/A",
        "Status": f"Failed: {error_msg}"
    })
    # raise e # Uncomment if you want the pipeline to show 'Failed' status immediately
mssparkutils.notebook.exit(json.dumps(package_stats))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# import xml.etree.ElementTree as ET
# import os

# # Define directories
# SOURCE_DIR = f"/lakehouse/default/Files/Package Templates/{entity_name}"
# DEST_DIR = f"/lakehouse/default/Files/Package Processed/{entity_name}"

# def create_updated_header(source_path, output_folder, new_description):
#     """
#     Reads the source Header XML, updates the Description, and saves to the output folder.
#     """
#     # 1. Register namespaces to keep the XML clean (prevents ns0: prefix)
#     ns = "http://schemas.microsoft.com/dynamics/2015/01/DataManagement"
#     ET.register_namespace('', ns)
#     ET.register_namespace('i', "http://www.w3.org/2001/XMLSchema-instance")
    
#     # 2. Parse the original file
#     tree = ET.parse(source_path)
#     root = tree.getroot()
    
#     # 3. Update the Description
#     desc_tag = root.find(f"{{{ns}}}Description")
#     if desc_tag is not None:
#         desc_tag.text = new_description
        
#     # 4. Prepare Output Path
#     if not os.path.exists(output_folder):
#         os.makedirs(output_folder)
        
#     output_path = os.path.join(output_folder, "PackageHeader.xml")
    
#     # 5. Save to the new location
#     tree.write(output_path, encoding="utf-8", xml_declaration=True)
#     print(f"Header saved to: {output_path}")

# def create_updated_manifest(source_path, output_folder, new_description, new_def_group_name):
#     """
#     Reads the source Manifest XML, updates Description and DefinitionGroupName, 
#     and saves to the output folder.
#     """
#     # 1. Register namespaces
#     ns = "http://schemas.microsoft.com/dynamics/2015/01/DataManagement"
#     ET.register_namespace('', ns)
#     ET.register_namespace('i', "http://www.w3.org/2001/XMLSchema-instance")
    
#     # 2. Parse the original file
#     tree = ET.parse(source_path)
#     root = tree.getroot()
    
#     # 3. Update the tags
#     desc_tag = root.find(f"{{{ns}}}Description")
#     if desc_tag is not None:
#         desc_tag.text = new_description

#     def_name_tag = root.find(f"{{{ns}}}DefinitionGroupName")
#     if def_name_tag is not None:
#         def_name_tag.text = new_def_group_name
        
#     # 4. Prepare Output Path
#     if not os.path.exists(output_folder):
#         os.makedirs(output_folder)

#     output_path = os.path.join(output_folder, "Manifest.xml")
    
#     # 5. Save to the new location
#     tree.write(output_path, encoding="utf-8", xml_declaration=True)
#     print(f"Manifest saved to: {output_path}")


# # Define source file paths
# source_header = os.path.join(SOURCE_DIR, "PackageHeader.xml")
# source_manifest = os.path.join(SOURCE_DIR, "Manifest.xml")

# # Define new values
# val_description = f"Cycle {dm_cycle} Fabric Import {entity_name}"
# val_def_group = f"DMF_C{dm_cycle}_Import_{entity_name.replace(' ', '_')}"

# # 1. Run Header Function
# create_updated_header(source_header, DEST_DIR, val_description)

# # 2. Run Manifest Function (3 parameters + output folder)
# create_updated_manifest(source_manifest, DEST_DIR, val_description, val_def_group)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# import zipfile
# import shutil

# # --- Source Directories ---
# dir_package_templates = f"/lakehouse/default/Files/Package Processed/{entity_name}"
# dir_csv_source = f"/lakehouse/default/Files/FO Cleansed/{entity_name}"

# # --- Destination Directory ---
# dir_zip_dest = "/lakehouse/default/Files/Zipped Packages"
# zip_filename = f"{entity_name}.zip"
# zip_full_path = os.path.join(dir_zip_dest, zip_filename)

# print(f"Packaging Entity: {entity_name}")
# print(f"Header/Manifest Source: {dir_package_templates}")
# print(f"CSV Source: {dir_csv_source}")
# print(f"Destination: {zip_full_path}")

# # Ensure destination folder exists
# if not os.path.exists(dir_zip_dest):
#     os.makedirs(dir_zip_dest)

# # Define the specific files we need to bundle
# files_to_zip = [
#     # (Full Source Path, Name inside Zip)
#     (os.path.join(dir_package_templates, "PackageHeader.xml"), "PackageHeader.xml"),
#     (os.path.join(dir_package_templates, "Manifest.xml"), "Manifest.xml"),
#     (os.path.join(dir_csv_source, f"{entity_name}.csv"), f"{entity_name}.csv")
# ]

# # Verify source files exist before attempting to zip
# missing_files = []
# for src, name in files_to_zip:
#     if not os.path.exists(src):
#         missing_files.append(src)

# if missing_files:
#     print(f"❌ Error: The following source files are missing:")
#     for f in missing_files:
#         print(f"   - {f}")
#     print("Stopping execution.")
# else:
#     # CREATE ZIP FILE
#     try:
#         with zipfile.ZipFile(zip_full_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
#             for src_path, arcname in files_to_zip:
#                 print(f"   Adding: {arcname}")
#                 # arcname ensures files are at the root of the zip (required for D365)
#                 zipf.write(src_path, arcname=arcname)
        
#         print(f"Success! Package saved to: {zip_full_path}")
        
#     except Exception as e:
#         print(f"An error occurred while creating the zip file: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
