import os
from concurrent.futures import ThreadPoolExecutor
from azure.storage.blob import BlobServiceClient, ContentSettings

# Global variables
azure_storage_connection_string = "DefaultEndpointsProtocol=https;AccountName=<PROVIDE ACCOUNT NAME HERE>;AccountKey=<PROVIDE ACCOUNT KEY>"
logs_directory = "<PROVIDE YOUR LOG DIRECTORY>"  # Replace this with the appropriate path or use GUI input
container_name = "<PROVIDE YOUR CONTAINER NAME CREATED UNDER AZURE STORAGE>"
max_workers = 5  # This value can be adjusted or set via GUI input in the future

# Function to upload a single file to Azure Blob Storage
def upload_file_to_blob(file_path):
    try:
        # Create a BlobServiceClient
        blob_service_client = BlobServiceClient.from_connection_string(azure_storage_connection_string)
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=os.path.basename(file_path))

        # Upload the file
        with open(file_path, "rb") as data:
            blob_client.upload_blob(data, overwrite=True, content_settings=ContentSettings(content_type='text/plain'))
        
        print(f"Successfully uploaded {file_path} to {container_name}")
    except Exception as e:
        print(f"Error uploading {file_path}: {e}")

# Function to process files in parallel
def upload_files_in_parallel(file_paths):
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(upload_file_to_blob, file_path) for file_path in file_paths]
        for future in futures:
            future.result()

# Main function
def main():
    # List all files in the provided directory
    file_paths = [os.path.join(logs_directory, file) for file in os.listdir(logs_directory) if os.path.isfile(os.path.join(logs_directory, file))]

    # Call the parallel upload function
    upload_files_in_parallel(file_paths)

if __name__ == "__main__":
    # Run the main function
    main()
