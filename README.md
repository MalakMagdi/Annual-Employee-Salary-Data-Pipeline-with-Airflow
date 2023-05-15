# Annual-Employee-Salary-Data-Pipeline-with-Airflow

It is an Apache Airflow project that automates the extraction, transformation, and loading (ETL) process for synchronizing employee salary data. The project extracts data from a PostgreSQL source system hosted on AWS, stages it in Amazon S3, performs transformations using Pandas, and updates and inserts the data into the Snowflake data warehouse.

![Project](https://github.com/MalakMagdi/Annual-Employee-Salary-Data-Pipeline-with-Airflow/assets/110945022/d89e3184-4c22-40dd-9116-cafc7f7f9a08)

## Project Overview

The primary goal of The project is to ensure accurate and up-to-date employee salary information in the data warehouse. The project performs the following steps:

1. Extraction: It uses the `PostgresOperator` to extract employee salary data from the PostgreSQL source system embedded in AWS.

2. Staging: The extracted data is staged in the Amazon S3 staging layer using the `S3UploadOperator`. This operator uploads the data to a designated S3 bucket.

3. Transformation: It applies transformations to the extracted data using the powerful data manipulation capabilities of Pandas. The transformations are performed within the `PythonOperator`, allowing you to customize the logic in the script.

4. Update and Insert: FlowSync uses the `SnowflakeOperator` to update the existing employee salary records in the Snowflake data warehouse. It checks if the employee record already exists in the data warehouse using a `BranchPythonOperator`. If the record exists, it performs an update; otherwise, it inserts the new record.

## Prerequisites

To run the project and execute the synchronization process, ensure you have the following:

- Access to the PostgreSQL source system hosted on AWS, with appropriate credentials and permissions.
- An AWS account with access to Amazon S3 for staging data storage.
- Snowflake account credentials and access to the target data warehouse.
- A compatible operating system (Windows, Linux, macOS) with Python and the necessary dependencies installed.
