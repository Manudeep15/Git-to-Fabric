# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse_name": "",
# META       "default_lakehouse_workspace_id": ""
# META     },
# META     "environment": {
# META       "environmentId": "894291a9-f8d5-4ea0-8d4d-a41d3d9b7541",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# CELL ********************

%run nb_helper_functions_parent_caller

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Constants for authentication
KV_TENANT_ID = 'api-ad-tenant-id'
KV_CLIENT_ID = 'api-ad-client-id'
KV_CLIENT_SECRET = 'api-ad-client-secret'

# Retrieve tenant_id, client_id, and client_secret
tenant_id = extract_secret(KV_TENANT_ID)
client_id = extract_secret(KV_CLIENT_ID)
client_secret = extract_secret(KV_CLIENT_SECRET)
scope = 'https://storage.azure.com/.default'

def list_files_in_onelake(dq_checks_source_storage_url, auth_token):
    """
    List all files in a directory in OneLake.
    """
    headers = {
        'Authorization': f'Bearer {auth_token}',
        'Accept': 'application/json'
    }

    response = requests.get(dq_checks_source_storage_url, headers=headers)
    logging.debug(f"Status Code: {response.status_code}")
    logging.debug(f"Response Content: {response.content.decode('utf-8')}")

    if not response.content:
        logging.warning("The response is empty. Check if the directory exists or the endpoint is correct.")
        return []

    try:
        response_data = response.json()
    except ValueError:
        logging.error("The response is not in JSON format. Verify if the API endpoint is correct for listing files.")
        return []

    files = response_data.get('paths', [])
    for file in files:
        if not file.get('isDirectory'):
            yield file['name']

def download_file_from_onelake(file_name_cleaned, auth_token):
    """
    Download a file's content from OneLake.
    """
    headers = {
        'Authorization': f'Bearer {auth_token}',
        'Accept': 'application/octet-stream'
    }

    file_url = (
        f"https://onelake.dfs.fabric.microsoft.com/"
        f"gs_tdm_dev/lh_cleansed.lakehouse/{file_name_cleaned}"
    )

    response = requests.get(file_url, headers=headers)

    if response.status_code == 200:
        return response.content
    else:
        logging.error(
            f"Failed to download {file_name_cleaned}: "
            f"{response.status_code} - {response.text}"
        )
        return None

def upload_file(file_name, upload_url, auth_token, relative_path):
    """
    Upload a single file to the specified OneLake location.
    """
    file_name_cleaned = file_name.split('/', 1)[-1]
    full_upload_path = f"{upload_url}{file_name_cleaned}".replace("\\", "/")

    headers = {
        'Authorization': f'Bearer {auth_token}',
        'Content-Type': 'application/octet-stream',
        'x-ms-blob-type': 'BlockBlob'
    }

    file_data = download_file_from_onelake(file_name_cleaned, auth_token)

    if not file_data:
        logging.error(f"Failed to retrieve {file_name_cleaned}. Skipping.")
        return

    response = requests.put(full_upload_path, headers=headers, data=file_data)

    if response.status_code == 201:
        logging.info(f"Successfully uploaded {file_name_cleaned} to {full_upload_path}")
    elif response.status_code == 404:
        logging.error(f"Failed to upload {file_name_cleaned}: Folder not found at {full_upload_path}.")
    else:
        logging.error(
            f"Failed to upload {file_name_cleaned}: "
            f"{response.status_code} - {response.text}"
        )

def main():
    """
    Main function to execute the file transfer from source to destination in OneLake.
    Note that the global variables 'env', 'dq_checks_target_storage_url' and 'dq_checks_source_storage_url' are defined in global configs.
    """
    # Define source and destination directories
    dq_checks_source_storage_url = globals()['global_configs']['dq_checks_source_storage_url']
    dq_checks_target_storage_url = globals()['global_configs']['dq_checks_target_storage_url']

    # Step 1: Retrieve the authentication token using the function defined in nb_helper_functions_metadata
    auth_token = get_api_token(tenant_id, client_id, client_secret, scope)

    # Step 2: List files in the OneLake source directory and upload each file to the destination
    for file_name in list_files_in_onelake(dq_checks_source_storage_url, auth_token):
        relative_path = os.path.dirname(file_name.split('/', 1)[-1])
        upload_file(file_name, dq_checks_target_storage_url, auth_token, relative_path)

    logging.info("All files have been uploaded successfully.")

if __name__ == '__main__':
    main()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
