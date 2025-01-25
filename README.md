# Formula 1 Season Analysis Data Pipeline Project 🚦🏎️
This project demonstrates a data pipeline designed to process and analyze Formula 1 race results data using a combination of modern data engineering tools and cloud services. The pipeline ingests raw data from the ergast API, processes and stores it in AWS S3, and enables querying and visualization with Athena and QuickSight.

# Features 🚀
### 1. Data Ingestion:

- Fetches Formula 1 race data from the Ergast API using an Airflow DAG.
- Raw data is dumped into an S3 bucket for storage.
- There are 2 dags created, one setup to ingest the latest available results from the api.
- The other dag to ingest historical race results data either for a specified race or a complete season.
- The pipline is made to be idempotent ensuring that reprocessing a certain result doesn't create duplicates in our storage location ensuring safety during backfills. 
### 2. Data Processing:

- A Lambda function is triggered on an S3 put event to clean and process the raw data and store the processed results in another processed S3 bucket in parquet.
### 3. Data Cataloging:
- AWS Glue Crawler is used to catalog the processed data.

### 4. Data Querying & Visualization:
- Processed data is queried using AWS Athena.
- Visualized to gain season stats and insights using Amazon QuickSight.

### 5. Tracking Processed Data:
- Airflow uses PostgreSQL as its backend.
- A processed_races table tracks the last processed races and their dates to help understand availability of new race data to ingest from the api.
- The processed_races table enables us to prevent redundant processing of the data.

# Architecture 🏗️

![image of software componeents and flow](./software_flow_v1.png)

The following components form the architecture of the project:
### 1. Programming Language
- Python
### 2. Containzerization and Orchestration:
- **Docker** is used to containerize the airflow instance with postgres backend.
- **Airflow** Manages the workflow of data ingestion and tracking.
- **PostgreSQL** serves as the Airflow metadata database and local DB.
### 3. AWS Services:

- **S3**: Storage for raw and processed race data.
- **Lambda**: Processes raw data and moves it to the processed S3 bucket.
- **Glue Crawler**: Creates a data catalog for Athena queries.
- **Athena**: Enables SQL-like queries on the processed data.
- **QuickSight**: Visualizes the processed data for insights
