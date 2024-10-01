# Log Collection, Ingestion, Parsing, and Reporting

This project consists of a series of Python scripts designed to automate the collection, ingestion, parsing, and reporting of log files from various sources such as Azure Blob Storage and SQL databases. The system also integrates predictive modeling to forecast potential future issues.

## Project Structure

1. **1LogCollector_v1.py**
   - This script uploads log files from a local directory to Azure Blob Storage.
   - **Key features**:
     - Multi-threaded file uploads.
     - Global variable configuration for Azure Storage connection and log directory paths.

2. **2logIngestion_v9_SysMessageintegration.py**
   - This script reads log files from Azure Blob Storage, filters error logs, and ingests them into a SQL Server database.
   - **Key features**:
     - Log filtering based on error codes and severity.
     - Multi-threaded processing of log files.
     - Ingests data into the `LogMessages` table in the SQL Server database.

3. **3LogParsing_AD_CC_PM_v3.py**
   - This script processes the log messages and performs anomaly detection using machine learning algorithms.
   - **Key features**:
     - Uses DBSCAN clustering for anomaly detection.
     - Performs feature engineering using TF-IDF.
     - Executes predictive modeling using `Prophet`.
     - Saves parsed logs and forecast results into the database.

4. **4generatereport_v2.py**
   - This script generates a text report based on the processed log data and forecasts.
   - **Key features**:
     - Generates server-wise issue summaries.
     - Provides detailed logs and forecasting summaries for potential future issues.
     - Outputs the report to a text file.

## Setup

1. Clone this repository to your local machine:
   ```bash
   git clone https://github.com/yourusername/log-processing-system.git

2.Install the required Python dependencies:
pip install -r requirements.txt

3.Configure your Azure Blob Storage and SQL Server database connection strings in each script:
azure_storage_connection_string
sql_conn_str

4.Run each script as needed:
Upload log files : python 1LogCollector_v1.py
ngest and process logs: python 2logIngestion_v9_SysMessageintegration.py
Parse logs and perform anomaly detection: python 3LogParsing_AD_CC_PM_v3.py
Generate reports: python 4generatereport_v2.py

Requirements
Python 3.8+
Azure SDK for Python
SQLAlchemy
scikit-learn
fbprophet (Prophet for forecasting)
pyodbc

To install the dependencies, use the requirements.txt file.
