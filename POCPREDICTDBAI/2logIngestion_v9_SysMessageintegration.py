"""
2logIngestion_v9_SysMessageintegration.py

Description:
This script reads logs from Azure Blob Storage, filters out relevant error logs, and inserts them into a SQL Server database.
It supports multi-threaded log processing to ensure high performance.

Key Features:
- Filters error logs based on error codes and severity.
- Multi-threaded log processing and batch inserts into SQL Server.
- Connection details for Azure Storage and SQL Server must be updated in the script.

Requirements:
- Python 3.8+
- Azure SDK for Python (azure-storage-blob)
- pyodbc for SQL Server connection
- Modify the azure_storage_connection_string and sql_conn_str variables.

Usage:
Run the script to ingest logs from Azure Blob Storage and insert them into the SQL database.
    python 2logIngestion_v9_SysMessageintegration.py
"""

import getpass
import threading
import concurrent.futures
from azure.storage.blob import BlobServiceClient
import pyodbc
from datetime import datetime
import logging
import os
import re
import chardet

# Disable oneDNN custom operations
os.environ['TF_ENABLE_ONEDNN_OPTS'] = '0'

# Get the username of the person executing the script
executing_user = getpass.getuser()

# Configure logging to file
logging.basicConfig(
    filename='extraction_audit.log',
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Azure Storage Connection String and SQL Server Connection String
azure_storage_connection_string = "<PROVIDE YOUR CONNECTION STRING>"
sql_conn_str = "<PROVIDE YOUR CONNECTION STRING OR ODBC DSN>"

# Lock for thread-safe database access
db_lock = threading.Lock()

# Adjustable parameters
MAX_WORKERS = 16
BATCH_SIZE = 1000

def extract_db_connection_info(conn_str):
    """
    Extracts and logs database connection information from the provided connection string.
    """
    try:
        server_match = re.search(r"DSN=([^;]+)", conn_str)
        user_match = re.search(r"UID=([^;]+)", conn_str)

        if server_match and user_match:
            server_name = server_match.group(1)
            user_name = user_match.group(1)
            logging.info(f"Database connection established : DSN :'{server_name}' | user :'{user_name}'.")
        else:
            if not server_match:
                logging.warning("Failed to extract server name from the connection string.")
            if not user_match:
                logging.warning("Failed to extract user name from the connection string.")
            logging.error("Incomplete database connection information. Please check the connection string format.")

    except Exception as e:
        logging.error(f"An error occurred while extracting database connection information: {str(e)}")

def filter_log_lines(lines):
    """
    Filters log lines to only include those with error codes and severities.
    """
    filtered_lines = []
    for line in lines:
        if "Error:" in line and "Severity:" in line:
            filtered_lines.append(line)
    return filtered_lines

def detect_file_encoding(data):
    """
    Detects the encoding of the given data.
    """
    try:
        result = chardet.detect(data)
        encoding = result['encoding']
        logging.info(f"Detected file encoding: {encoding}")
        return encoding
    except Exception as e:
        logging.error(f"An error occurred while detecting file encoding: {str(e)}")
        return None

def read_blob_logs():
    """
    Reads logs from Azure Blob Storage and processes each blob.
    """
    try:
        blob_service_client = BlobServiceClient.from_connection_string(azure_storage_connection_string)
        container_client = blob_service_client.get_container_client("logs")

        blobs = list(container_client.list_blobs())
        logging.info(f"Found {len(blobs)} blobs in the container.")

        if not blobs:
            logging.warning("No blobs found in the container. Exiting.")
            return

        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [executor.submit(process_blob, blob) for blob in blobs]
            concurrent.futures.wait(futures)

    except Exception as e:
        logging.error(f"An error occurred while reading blob logs: {str(e)}")

def process_blob(blob):
    """
    Processes each log file (blob) by extracting its content and filtering the relevant lines.
    """
    try:
        logging.info(f"Processing blob: {blob.name}")

        servername = blob.name.split('_')[0]
        LogFileName = blob.name
        LogFileSize = round(blob.size / 1024, 2)
        logfiletype = "errorlog" if "errorlog" in LogFileName.lower() else "sqlagent" if "sqlagent" in LogFileName.lower() else "other"
        source = "database"
        db_type = "MSSQL"
        logfile_gen_date = blob.creation_time
        LogExtractedTime = datetime.now()

        blob_client = BlobServiceClient.from_connection_string(azure_storage_connection_string).get_blob_client(container="logs", blob=blob.name)
        download_stream = blob_client.download_blob()
        raw_data = download_stream.readall()

        # Detect file encoding
        encoding = detect_file_encoding(raw_data)
        if not encoding:
            encoding = 'utf-8'

        try:
            log_content = raw_data.decode(encoding)
        except UnicodeDecodeError as e:
            logging.error(f"Decoding error with {encoding} encoding: {str(e)}. Trying with fallback encoding 'ISO-8859-1'.")
            log_content = raw_data.decode('ISO-8859-1', errors='replace')

        logging.info(f"Downloaded log file: {LogFileName} from server: {servername}")

        if "SQL Server" not in log_content:
            db_type = "other"

        log_id = insert_log_details(servername, LogFileName, LogFileSize, logfiletype, source, db_type, logfile_gen_date, LogExtractedTime)
        
        if log_id:
            lines = log_content.splitlines()
            filtered_lines = filter_log_lines(lines)
            log_entries = []

            for line in filtered_lines:
                log_entry = process_log_line(log_id, line)
                if log_entry:
                    log_entries.append(log_entry)

                if len(log_entries) >= BATCH_SIZE:
                    batch_insert_log_lines(log_entries)
                    log_entries = []

            if log_entries:
                batch_insert_log_lines(log_entries)

    except Exception as e:
        logging.error(f"An error occurred while processing blob: {str(e)}")

def batch_insert_log_lines(log_entries):
    """
    Batch inserts log entries into the LogMessages table.
    """
    try:
        with db_lock:
            connection = pyodbc.connect(sql_conn_str)
            cursor = connection.cursor()

            insert_query = """
            INSERT INTO [dbo].[LogMessages] (LogID, LogDate, LogMessageType, ErrorCode, Severity, LogMessage, Euser)
            VALUES (?, ?, ?, ?, ?, NULL, ?)
            """

            log_entries_with_user = [(log_id, log_date, log_message_type, error_code, severity, executing_user) 
                                     for (log_id, log_date, log_message_type, error_code, severity) in log_entries]

            cursor.executemany(insert_query, log_entries_with_user)
            connection.commit()

            cursor.close()
            connection.close()

            logging.info(f"Batch insert of {len(log_entries)} log lines completed.")
    except Exception as e:
        logging.error(f"An error occurred during batch insertion: {str(e)}")

def insert_log_details(servername, LogFileName, LogFileSize, logfiletype, source, db_type, logfile_gen_date, LogExtractedTime):
    """
    Inserts details of the processed log file into the LogDetails table.
    """
    try:
        connection = pyodbc.connect(sql_conn_str)
        cursor = connection.cursor()

        insert_query = """
        INSERT INTO [dbo].[LogDetails] (ServerName, LogFileName, LogFileSize, LogFileType, Source, DB_Type, logfile_gen_date, LogExtractedTime, Euser)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        cursor.execute(insert_query, (servername, LogFileName, LogFileSize, logfiletype, source, db_type, logfile_gen_date, LogExtractedTime, executing_user))
        connection.commit()

        cursor.execute("SELECT @@IDENTITY AS LogID")
        log_id = cursor.fetchone()[0]

        logging.info(f"Inserted log details for file: {LogFileName} with LogID: {log_id}")

        cursor.close()
        connection.close()

        return log_id

    except Exception as e:
        logging.error(f"An error occurred while inserting log details: {str(e)}")
        return None

def process_log_line(log_id, line):
    """
    Processes each log line to extract relevant information including ErrorCode and Severity.
    """
    try:
        if not line.strip():
            return None

        match = re.search(r"Error:\s*(\d+),\s*Severity:\s*(\d+)", line)
        if not match:
            return None

        error_code = int(match.group(1))
        severity = int(match.group(2))

        parts = line.split(' ', 2)
        if len(parts) < 3:
            return None

        try:
            log_date_str = parts[0] + ' ' + parts[1]
            log_date = datetime.strptime(log_date_str, "%Y-%m-%d %H:%M:%S.%f")
        except ValueError:
            logging.warning(f"Skipping line due to invalid date format: {line}")
            return None

        log_message_type = "error"
        return (log_id, log_date, log_message_type, error_code, severity)

    except Exception as e:
        logging.error(f"An error occurred while processing log line: {str(e)}")
        return None

def execute_stored_procedure():
    """
    Executes the stored procedure to update LogMessage in the LogMessages table.
    """
    try:
        with db_lock:
            connection = pyodbc.connect(sql_conn_str)
            cursor = connection.cursor()

            # Execute the stored procedure
            cursor.execute("EXEC UpdateLogMessages")
            connection.commit()

            cursor.close()
            connection.close()
            logging.info("Stored procedure executed successfully to update LogMessage.")
    except Exception as e:
        logging.error(f"An error occurred while executing the stored procedure: {str(e)}")

if __name__ == "__main__":
    logging.info("Log extraction process started.")
    
    # Extract and log database connection details
    extract_db_connection_info(sql_conn_str)

    # Start the log reading process
    read_blob_logs()

    # Execute the stored procedure to update LogMessage
    execute_stored_procedure()

    logging.info("Log extraction process completed.")
    logging.info(">>-----------------------------------------------<<")
