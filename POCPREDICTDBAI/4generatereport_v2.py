import pandas as pd
from sqlalchemy import create_engine, text
import urllib

# Connection details
connection_string = "<PROVIDE YOUR ODBC DSN AND CREDENTIALS>"
params = urllib.parse.quote_plus(connection_string)
sql_conn_str = f"mssql+pyodbc:///?odbc_connect={params}"

# Function to call the stored procedure and get data
def get_data_from_stored_procedure():
    try:
        # Create SQLAlchemy engine
        engine = create_engine(sql_conn_str)

        # Execute the stored procedure
        query = text("EXEC GetEndUserLogReport")
        detailed_log_df = pd.read_sql(query, engine)
        print("Data loaded from stored procedure:", detailed_log_df.shape)

        return detailed_log_df

    except Exception as e:
        print(f"An error occurred while pulling data from SQL: {e}")
        return pd.DataFrame()

# Function to load forecasting data from SQL Server
def load_forecast_data():
    try:
        # Create SQLAlchemy engine
        engine = create_engine(sql_conn_str)

        # Load data from ForecastResults table
        forecast_results_query = "SELECT * FROM ForecastResults"
        forecast_results_df = pd.read_sql(forecast_results_query, engine)

        return forecast_results_df

    except Exception as e:
        print(f"An error occurred while loading forecast data from SQL: {e}")
        return pd.DataFrame()

# Function to generate a user-friendly text report
def generate_text_report(detailed_log_df, forecast_results_df):
    report = []
    report.append("LOG PARSING AND FORECASTING REPORT\n")
    report.append("=" * 50 + "\n")

    # Section 1: Server-Wise Summary
    report.append("1. Server-Wise Issue Summary\n")
    server_wise_summary = detailed_log_df.groupby('ServerName')['IssueType'].value_counts().reset_index(name='Count')
    
    if server_wise_summary.empty:
        report.append("No issues found.\n")
    else:
        for server, group in server_wise_summary.groupby('ServerName'):
            report.append(f"\nServer: {server}\n")
            for _, row in group.iterrows():
                report.append(f"Issue: {row['IssueType']} - {row['Count']} occurrences\n")
    report.append("\n")

    # Remove duplicate entries for Section 2
    unique_logs_df = detailed_log_df.drop_duplicates(subset=['ServerName', 'LogDate', 'ParsedMessage'])

    # Section 2: Detailed Logs by Server and Date
    report.append("2. Detailed Logs by Server and Date\n")
    for server, group in unique_logs_df.groupby('ServerName'):
        report.append(f"\nServer: {server}\n")
        for _, row in group.iterrows():
            report.append(f"Date: {row['LogDate']}, Issue: {row['IssueType']}, Message: {row['ParsedMessage']}\n")
    report.append("\n")

    # Section 3: Forecasting Summary
    report.append("3. Forecasting Summary\n")
    future_forecasts = forecast_results_df[forecast_results_df['ds'] >= pd.Timestamp.now()]
    
    if future_forecasts.empty:
        report.append("No future forecasts available.\n")
    else:
        max_forecast = future_forecasts.sort_values(by='yhat', ascending=False).head(5)
        report.append("Top 5 Dates with Highest Predicted Issue Counts:\n")
        for _, row in max_forecast.iterrows():
            report.append(f"Date: {row['ds']}, Predicted Issue Count: {row['yhat']:.2f}, Confidence Interval: [{row['yhat_lower']:.2f}, {row['yhat_upper']:.2f}]\n")

        report.append("\nGeneral Forecast Trends:\n")
        trend_summary = forecast_results_df.groupby('trend').size().reset_index(name='Count')
        for _, row in trend_summary.iterrows():
            report.append(f"Trend Value: {row['trend']}, Count: {row['Count']} occurrences\n")
    
    report.append("\n")

    # Save the report to a text file
    with open("End_User_Log_Report.txt", "w") as file:
        file.writelines(report)

    print("Report generated successfully: End_User_Log_Report.txt")

# Main function to execute the process
def main():
    # Load data from stored procedure
    detailed_log_df = get_data_from_stored_procedure()

    # Load forecast data from SQL Server
    forecast_results_df = load_forecast_data()

    # Check if data is loaded
    if detailed_log_df.empty or forecast_results_df.empty:
        print("No data loaded. Exiting process.")
        return

    # Generate text report
    generate_text_report(detailed_log_df, forecast_results_df)

if __name__ == "__main__":
    main()
