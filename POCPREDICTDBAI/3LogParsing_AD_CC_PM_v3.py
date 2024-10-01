import pandas as pd
from sklearn.cluster import DBSCAN
from sklearn.feature_extraction.text import TfidfVectorizer
from prophet import Prophet
from sqlalchemy import create_engine, text
import urllib

# Correct SQLAlchemy connection string using DSN with `odbc_connect`
connection_string = "<PROVIDE YOUR ODBC DSN AND CREDENTIALS>"
params = urllib.parse.quote_plus(connection_string)
sql_conn_str = f"mssql+pyodbc:///?odbc_connect={params}"

# Function to call the stored procedure and get data
def get_data_from_stored_procedure():
    try:
        # Create SQLAlchemy engine
        engine = create_engine(sql_conn_str)

        # Call the stored procedure to fetch data
        query = "EXEC GetErrorLogsWithDetails"
        log_messages_df = pd.read_sql(query, engine)
        print("Data loaded from stored procedure:", log_messages_df.shape)
        
        return log_messages_df

    except Exception as e:
        print(f"An error occurred while pulling data from SQL: {e}")
        return pd.DataFrame()

# Function to parse log messages and categorize issues
def parse_log_message(log_message):
    # Example parsing logic - categorize based on log message content
    if "timeout" in log_message.lower():
        log_template = "Timeout error while waiting for resources"
        parsed_message = "A timeout occurred due to resource contention."
        issue_type = "Memory Resource Issue"
    elif "SSPI handshake failed" in log_message:
        log_template = "SSPI handshake failure"
        parsed_message = "SSPI handshake failed, likely due to network or authentication issues."
        issue_type = "Network or Authentication Issue"
    elif "Login failed" in log_message:
        log_template = "Login failure"
        parsed_message = "Login failed due to an untrusted domain or incorrect credentials."
        issue_type = "Authentication Issue"
    else:
        log_template = "Other"
        parsed_message = log_message  # Default to the original message if no pattern matches
        issue_type = "General Issue"

    return log_template, parsed_message, issue_type

# Function for feature engineering
def feature_engineering(log_messages_df):
    print("Sample content from LogMessage column:")
    print(log_messages_df['LogMessage'].head(10))

    if log_messages_df['LogMessage'].isnull().all() or log_messages_df['LogMessage'].str.strip().eq('').all():
        print("LogMessage column is empty or contains only stop words. Skipping TF-IDF vectorization.")
        tfidf_features = pd.DataFrame()  
    else:
        log_messages_df['LogMessageType_Encoded'] = pd.factorize(log_messages_df['LogMessageType'])[0]
        vectorizer = TfidfVectorizer(max_features=50, stop_words=None)
        tfidf_matrix = vectorizer.fit_transform(log_messages_df['LogMessage'].fillna('')).toarray()
        tfidf_features = pd.DataFrame(tfidf_matrix, columns=vectorizer.get_feature_names_out())

    if not tfidf_features.empty:
        combined_features = pd.concat([log_messages_df[['LogMessageType_Encoded']], tfidf_features], axis=1)
    else:
        combined_features = log_messages_df[['LogMessageType_Encoded']]
    
    print("Features engineered:", combined_features.shape)
    return combined_features

# Function for anomaly detection using DBSCAN
def detect_anomalies(log_messages_df, features_df):
    if log_messages_df.empty:
        print("No log messages available for anomaly detection.")
        return log_messages_df

    clustering = DBSCAN(eps=1.0, min_samples=3).fit(features_df)
    log_messages_df['Cluster'] = clustering.labels_
    log_messages_df['AnomalyScore'] = 1  # Flag all entries as issues
    
    print("Anomalies detected:", log_messages_df['AnomalyScore'].sum())
    return log_messages_df

# Function to save processed data back to SQL
def save_to_database(processed_data_df, table_name):
    if processed_data_df.empty:
        print(f"No data to save to {table_name}.")
        return

    # Debug: Print a sample of the data being saved
    print(f"Data to save to {table_name}:")
    print(processed_data_df.head())

    try:
        engine = create_engine(sql_conn_str)
        with engine.connect() as conn:
            transaction = conn.begin()
            try:
                for _, row in processed_data_df.iterrows():
                    result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name} WHERE LogID = :logid"), {'logid': row['LogID']})
                    if result.scalar() == 0:  # Only insert if not already present
                        conn.execute(text(f"""
                        INSERT INTO {table_name} (LogID, LogDate, LogMessageType, LogMessage, LogTemplate, ParsedMessage, AnomalyScore, Cluster, IssueType)
                        VALUES (:logid, :logdate, :logtype, :logmessage, :logtemplate, :parsed, :score, :cluster, :issuetype)
                        """), {
                            'logid': row['LogID'], 
                            'logdate': row['LogDate'], 
                            'logtype': row['LogMessageType'], 
                            'logmessage': row['LogMessage'], 
                            'logtemplate': row['LogTemplate'], 
                            'parsed': row.get('ParsedMessage', ''), 
                            'score': row.get('AnomalyScore', 1), 
                            'cluster': row['Cluster'],
                            'issuetype': row['IssueType']
                        })
                transaction.commit()
                print(f"Data successfully saved to {table_name}.")
            except Exception as e:
                transaction.rollback()
                print(f"An error occurred while saving data to SQL: {e}")
    except Exception as e:
        print(f"An error occurred while connecting to the database: {e}")

# Function to prepare data for Prophet
def prepare_data_for_prophet(log_messages_df):
    if log_messages_df.empty:
        print("No data available for predictive modeling.")
        return pd.DataFrame()

    if 'LogDate' not in log_messages_df.columns or 'AnomalyScore' not in log_messages_df.columns:
        print("Required columns for Prophet ('LogDate', 'AnomalyScore') are missing.")
        return pd.DataFrame()

    prophet_data = log_messages_df[['LogDate', 'AnomalyScore']].rename(columns={'LogDate': 'ds', 'AnomalyScore': 'y'})
    print("Data prepared for Prophet:", prophet_data.shape)
    print("Sample of data prepared for Prophet:")
    print(prophet_data.head())

    return prophet_data

# Function for predictive modeling using Prophet
def predictive_modeling(prophet_data):
    if prophet_data.empty:
        print("No data available for Prophet modeling.")
        return pd.DataFrame()

    try:
        model = Prophet()
        model.fit(prophet_data)
        future = model.make_future_dataframe(periods=30)
        forecast = model.predict(future)
        print("Forecast completed:", forecast.shape)
        print("Sample of forecast data:")
        print(forecast.head())
        
        return forecast
    except Exception as e:
        print(f"An error occurred during predictive modeling: {e}")
        return pd.DataFrame()

# Function to save forecast results to SQL
def save_forecast_to_database(forecast):
    if forecast.empty:
        print("No forecast data to save.")
        return

    try:
        engine = create_engine(sql_conn_str)
        with engine.connect() as conn:
            transaction = conn.begin()
            try:
                for _, row in forecast.iterrows():
                    conn.execute(text(f"""
                    INSERT INTO ForecastResults (ds, yhat, yhat_lower, yhat_upper, trend, seasonality)
                    VALUES (:ds, :yhat, :yhat_lower, :yhat_upper, :trend, :seasonality)
                    """), {
                        'ds': row['ds'], 
                        'yhat': row['yhat'], 
                        'yhat_lower': row['yhat_lower'], 
                        'yhat_upper': row['yhat_upper'], 
                        'trend': row.get('trend', 0), 
                        'seasonality': row.get('seasonality', 0)
                    })
                transaction.commit()
                print("Forecast data successfully saved to ForecastResults.")
            except Exception as e:
                transaction.rollback()
                print(f"An error occurred while saving forecast to SQL: {e}")
    except Exception as e:
        print(f"An error occurred while connecting to the database: {e}")

# Main function to execute the process
def main():
    log_messages_df = get_data_from_stored_procedure()
    
    if log_messages_df.empty:
        print("No data loaded. Exiting process.")
        return

    log_messages_df[['LogTemplate', 'ParsedMessage', 'IssueType']] = log_messages_df['LogMessage'].apply(lambda msg: pd.Series(parse_log_message(msg)))

    features_df = feature_engineering(log_messages_df)
    
    log_messages_with_anomalies = detect_anomalies(log_messages_df, features_df)
    
    save_to_database(log_messages_with_anomalies, 'LogMessages_Processed')
    
    prophet_data = prepare_data_for_prophet(log_messages_with_anomalies)
    
    forecast = predictive_modeling(prophet_data)
    
    save_forecast_to_database(forecast)

if __name__ == "__main__":
    main()
