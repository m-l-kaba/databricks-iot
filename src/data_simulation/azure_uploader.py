"""
Azure Data Lake uploader for IoT data
"""

import json
import os
from typing import Dict, List, Any, Optional
from datetime import datetime
import logging

try:
    from azure.storage.filedatalake import DataLakeServiceClient
    from azure.identity import DefaultAzureCredential
except ImportError:
    print(
        "Azure SDK not installed. Run: pip install azure-storage-file-datalake azure-identity"
    )
    DataLakeServiceClient = None
    DefaultAzureCredential = None


class AzureDataLakeUploader:
    """Uploads IoT data to Azure Data Lake Gen2"""

    def __init__(
        self,
        storage_account_name: str,
        container_name: str = "raw",
        storage_account_key: Optional[str] = None,
    ):
        """
        Initialize Azure Data Lake uploader

        Args:
            storage_account_name: Name of the Azure storage account
            container_name: Name of the container (filesystem)
            storage_account_key: Storage account key (if not using managed identity)
        """
        if not DataLakeServiceClient:
            raise ImportError(
                "Azure SDK not installed. Run: pip install azure-storage-file-datalake azure-identity"
            )

        self.storage_account_name = storage_account_name
        self.container_name = container_name

        # Create service client
        if storage_account_key:
            account_url = f"https://{storage_account_name}.dfs.core.windows.net"
            self.service_client = DataLakeServiceClient(
                account_url=account_url, credential=storage_account_key
            )
        else:
            # Use managed identity or Azure CLI credentials
            account_url = f"https://{storage_account_name}.dfs.core.windows.net"
            credential = DefaultAzureCredential()
            self.service_client = DataLakeServiceClient(
                account_url=account_url, credential=credential
            )

        # Get filesystem client
        self.filesystem_client = self.service_client.get_file_system_client(
            file_system=container_name
        )

        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)

    def upload_json_data(
        self,
        data: List[Dict[str, Any]],
        folder_path: str,
        filename: Optional[str] = None,
    ) -> str:
        """
        Upload JSON data to a specific folder in the data lake

        Args:
            data: List of dictionaries to upload
            folder_path: Target folder path (e.g., 'telemetry', 'failures')
            filename: Optional filename, if not provided will use timestamp

        Returns:
            Full path where data was uploaded
        """
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"data_{timestamp}.json"

        file_path = f"{folder_path}/{filename}"

        # Convert data to JSON string
        json_data = json.dumps(data, indent=2)

        try:
            # Get file client
            file_client = self.filesystem_client.get_file_client(file_path)

            # Upload data
            file_client.upload_data(data=json_data, overwrite=True)

            self.logger.info(
                f"Successfully uploaded {len(data)} records to {file_path}"
            )
            return file_path

        except Exception as e:
            self.logger.error(f"Failed to upload data to {file_path}: {str(e)}")
            raise

    def upload_all_iot_data(
        self, iot_data: Dict[str, List[Dict[str, Any]]], timestamp_suffix: bool = True
    ) -> Dict[str, str]:
        """
        Upload all IoT data categories to their respective folders

        Args:
            iot_data: Dictionary with keys: telemetry, failures, maintenance, device_master
            timestamp_suffix: Whether to add timestamp to filenames

        Returns:
            Dictionary mapping data type to uploaded file path
        """
        uploaded_paths = {}
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S") if timestamp_suffix else ""

        for data_type, records in iot_data.items():
            if not records:
                self.logger.warning(f"No data found for {data_type}, skipping upload")
                continue

            # Create filename
            if timestamp_suffix:
                filename = f"{data_type}_{timestamp}.json"
            else:
                filename = f"{data_type}.json"

            try:
                uploaded_path = self.upload_json_data(
                    data=records, folder_path=data_type, filename=filename
                )
                uploaded_paths[data_type] = uploaded_path

            except Exception as e:
                self.logger.error(f"Failed to upload {data_type} data: {str(e)}")
                # Continue with other data types even if one fails
                continue

        return uploaded_paths

    def list_files_in_folder(self, folder_path: str) -> List[str]:
        """List all files in a specific folder"""
        try:
            paths = self.filesystem_client.get_paths(path=folder_path)
            files = [path.name for path in paths if not path.is_directory]
            return files
        except Exception as e:
            self.logger.error(f"Failed to list files in {folder_path}: {str(e)}")
            return []

    def download_file(self, file_path: str) -> str:
        """Download file content as string"""
        try:
            file_client = self.filesystem_client.get_file_client(file_path)
            download = file_client.download_file()
            content = download.readall().decode("utf-8")
            return content
        except Exception as e:
            self.logger.error(f"Failed to download {file_path}: {str(e)}")
            raise


def upload_iot_data_to_azure(
    iot_data: Dict[str, List[Dict[str, Any]]],
    storage_account_name: str,
    container_name: str = "raw",
    storage_account_key: Optional[str] = None,
) -> Dict[str, str]:
    """
    Convenience function to upload IoT data to Azure Data Lake

    Args:
        iot_data: Dictionary with IoT data categories
        storage_account_name: Azure storage account name
        container_name: Container name
        storage_account_key: Storage account key (optional)

    Returns:
        Dictionary mapping data type to uploaded file path
    """
    uploader = AzureDataLakeUploader(
        storage_account_name=storage_account_name,
        container_name=container_name,
        storage_account_key=storage_account_key,
    )

    return uploader.upload_all_iot_data(iot_data)
