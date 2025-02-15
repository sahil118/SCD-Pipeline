# Real-Time-SCD-Pipeline
## Project Overview:

CustomerTrack is a real-time data pipeline designed to extract and manage customer information, including name, city, state, country, and email. It implements both SCD (Slowly Changing Dimension) Type 1 and Type 2 methodologies. SCD Type 1 updates the customer data by overwriting existing records, while SCD Type 2 tracks historical data by maintaining multiple versions of customer records over time. The pipeline efficiently handles streaming data, ensuring accurate real-time updates while preserving a historical record of customer information for comprehensive analysis and reporting.

## Key Features & Technical Overview

* Real-time Data Pipeline: Extracts and processes customer data in real-time, ensuring up-to-date records are available for analysis and reporting.

* SCD Type 1 & Type 2: Implements SCD Type 1 for current data updates and SCD Type 2 for maintaining historical records, providing a comprehensive view of customer changes over time.

* EC2-Based Virtual Machine: Leverages EC2 instances as virtual machines for scalable and efficient data processing, ensuring high availability and flexibility in handling large data volumes.

* Data Versioning & Historical Tracking: Tracks and stores multiple versions of customer records, preserving historical changes for accurate reporting and analysis.


![Diagram](https://github.com/sahil118/SCD-Pipeline/blob/main/Screenshot%202025-02-15%20193843.png)

## Data Creation Module

* Custom Python Script with Faker Library: Utilized a custom Python script, powered by the Faker library, to generate synthetic customer data, including personal details like name, city, state, country, and email.

* Containerization with Docker: The entire data generation process is containerized using Docker, ensuring a consistent, scalable, and isolated environment for generating data.

## Data Ingetion

  * AWS EC2 instances for deploying and scaling cloud-based computing resources.
  * Apache NiFi for orchestrating and managing data workflows.
  * Apache ZooKeeper for overseeing distributed system coordination and synchronization.
  * Amazon S3 Bucket to store data for consolidating and securing large datasets.

## Data Insertion

Data Insertion Process:

* Snowflake Data Warehouse for Data Insertion: Utilized Snowflake as the data warehouse to store and manage large volumes of structured and semi-structured data.

* Snowpipe for Automated Staging Data: Implemented Snowpipe to automatically ingest and stage data in real-time from various sources into Snowflake, streamlining the data loading process.

* Snowflake Streams for Change Data Capture: Leveraged Snowflake Streams for efficiently tracking changes in data, enabling accurate change data capture (CDC) for real-time updates and synchronization.

* Snowflake Tasks for Automation: Used Snowflake Tasks to automate the data processing pipeline, scheduling and executing SQL queries for data transformation, loading, and refreshing on a defined schedule.

## Project Execution Details:

**Data Generation Using Faker Library** :

* A custom Python script powered by the Faker library generates synthetic customer data, including fields like name, city, state, country, and email. This data is saved as files, which are monitored by Apache NiFi.

**pache NiFi for Real-Time Monitoring** :

* Apache NiFi monitors the generated data files in real-time. Once the files are created, NiFi detects these files and automatically ingests them.

**Storing Data to Amazon S3 Bucket** :

* After NiFi picks up the data, it automatically stores the files in an Amazon S3 bucket, which serves as a central data lake for the system.

**Snowpipe for Automatic Data Loading to Snowflake** :

* Snowpipe is used to automatically detect when new files are added to the S3 bucket. Once a file arrives, Snowpipe loads it into a staging table in Snowflake.
* Staging Table: The customer_raw table is the staging table that temporarily holds the incoming raw customer data.

**Task for Regular Data Processing (Every 60 Minutes)** :

* A scheduled Snowflake Task runs every 60 minutes to check if there is any new data in the customer_raw staging table.
* This task calls a stored procedure which performs the data merge operation:
   * SCD Type 1 (Overwrite): If new data arrives with a matching customer_id, the existing record is updated. New records are simply inserted.

**Handling Slowly Changing Dimensions (SCD Type 2)** :

* A stream is created on the customer table to capture changes (insertions, updates) to customer records.
* Customer History Table: To maintain historical data, a customer_history table is created with additional fields:
  * start_time: Represents when the record became active.
  * end_time: Represents when the record was superseded by a newer version.
  * flag: A boolean flag (True/False) indicates whether the record is the active or historical version.
* When changes are captured by the stream, the customer_history table is updated with historical versions of customer data, allowing you to keep track of all changes over time.

**Tables, Streams and Snowpipe** :

**Tables** : 

###### 1. Customer_raw
###### 2. Customer
###### 3. Customer_history

### Stream 
```
create or replace stream customer_table_changes on table customer;
```
### Snowpipe 
```
   CREATE OR REPLACE PIPE SCD_DEMO.SCD2.customer_s3_pipe
   auto_ingest = TRUE
   as
   copy into SCD_DEMO.SCD2.CUSTOMER_RAW
   from @customer_ext_stage
   file_format = SCD_DEMO.SCD2.csv_file;
```

## Project Code :

### Data Generation Script 
```
def update_time():
    current_time = datetime.now().strftime("%Y%m%d%H%M%S")
    return current_time
def create_csv_file(time_stamp):
    with open(f'FakeDataset/customer_{time_stamp}.csv', 'w', newline='') as csvfile:
        fieldnames = ["customer_id","first_name","last_name","email","street",
                      "city","state","country"
                     ]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        for i in range(1,RECORD_COUNT+1):
            #print(i)
            writer.writerow(
                {
                    "customer_id": i,#fake.random_int(min=1, max=10000),
                    'first_name': fake.first_name(),
                    'last_name': fake.last_name(),
                    'email': fake.email(),
                    'street': fake.street_address(),
                    'city': fake.city(),
                    'state': fake.state(),
                    'country': fake.country()
                }
            )
if __name__ == '__main__':
    try:
        while (True):
            print("running code")
            time_stamp = update_time()       
            create_csv_file(time_stamp)
            time.sleep(3)
    except KeyboardInterrupt:
        print("Code execution stopped by user.")
    except Exception as e:
        print(f"Code has issue: {e}")
```
### SCD-1 Data Processing Code
```
CREATE OR REPLACE PROCEDURE pdr_scd_demo()
  RETURNS STRING
  LANGUAGE SQL
AS
$$
BEGIN
    -- Perform the MERGE operation between the 'customer' table and the 'customer_raw' staging table
    MERGE INTO customer c
    USING customer_raw cr
    ON c.customer_id = cr.customer_id

    -- Condition to detect if there's a difference between the existing customer data and the raw customer data
    WHEN MATCHED AND (
        c.customer_id <> cr.customer_id
        OR c.first_name <> cr.first_name
        OR c.last_name <> cr.last_name
        OR c.email <> cr.email
        OR c.street <> cr.street
        OR c.city <> cr.city
        OR c.state <> cr.state
        OR c.country <> cr.country
    )

-- The complete query can be found in the SQL file.
```

### SCD-2 Data Processing Code 
```
-- Start of the MERGE statement to synchronize data from source to target
MERGE INTO customer_history ch
USING v_customer_changing_data ccd
    -- Condition to match records between the target table (customer_history) and source view (v_customer_changing_data)
    ON ch.customer_id = ccd.customer_id
    AND ch.start_time = ccd.start_time

-- When a matching record is found and the operation is a delete (D) in the source data (ccd)
WHEN MATCHED AND ccd.dml_type = 'D' THEN
    -- Update the matched record in the target table (customer_history) to mark it as historical
    UPDATE
    SET ch.end_time = ccd.end_time,  -- Set the end_time to the value from the source (ccd)
        ch.is_current = FALSE         -- Mark the record as not current (historical)
    
-- When no matching record is found (new record in the source data)
WHEN NOT MATCHED THEN
    -- Insert a new record into the customer_history table with data from the source (ccd)
    INSERT (CUSTOMER_ID, FIRST_NAME, LAST_NAME, EMAIL, STREET, CITY, STATE, COUNTRY, START_TIME, END_TIME, IS_CURRENT)
    VALUES (ccd.customer_id, ccd.first_name, ccd.last_name, ccd.email, ccd.street, ccd.city, ccd.state, ccd.country, ccd.start_time, ccd.end_time, ccd.is_current);
    -- Insert the customer data with the relevant details, including the start_time, end_time, and the flag for current data

-- Return a message indicating the operation has been completed
RETURN 'COMPLETED';
-- End of the MERGE operation
END;

```
### Data Versioning 
```
CREATE OR REPLACE VIEW v_customer_changing_data AS (
    -- INSERT/FALSE: NEW ROW - Records that are new and should be inserted into the customer table
    WITH insert_row AS (
        SELECT 
            CUSTOMER_ID, 
            FIRST_NAME, 
            LAST_NAME,
            EMAIL, 
            STREET, 
            CITY,
            STATE,
            COUNTRY,
            UPDATE_TIMESTAMP,
            'I' AS DML_TYPE -- 'I' for Insert operation
        FROM scd_demo.scd2.customer_table_changes_i
        WHERE METADATA$ACTION = 'INSERT' -- Filter for insert actions
        AND METADATA$ISUPDATE = 'FALSE' -- Ensure that the data is not an update, but a new record
    ),
    
    -- UPDATE: The entry that will be marked as 'current' in the SCD2 table
    update_row_insert AS (
        SELECT 
            CUSTOMER_ID, 
            FIRST_NAME, 
            LAST_NAME,
            EMAIL, 
            STREET, 
            CITY,
            STATE,
            COUNTRY,
            UPDATE_TIMESTAMP,
            'U' AS DML_TYPE -- 'U' for Update operation
        FROM scd_demo.scd2.customer_table_changes_i
        WHERE METADATA$ACTION = 'INSERT' -- Filter for insert actions
        AND METADATA$ISUPDATE = 'TRUE' -- Mark as an update, for when existing data has changed

---- The complete query can be found in the SQL file.

```
### Apache nifi Data flow :
![Apache nifi Data flow](https://github.com/sahil118/SCD-Pipeline/blob/main/Screenshot%202025-02-14%20204846.png)

### SCD-1 
![SCD-1](https://github.com/sahil118/SCD-Pipeline/blob/main/Screenshot%202025-02-15%20165117.png)

### SCD-2
![SCD-2](https://github.com/sahil118/SCD-Pipeline/blob/main/Screenshot%202025-02-15%20165038.png)
## Use Cases:

**Customer Data Management**:

This pipeline is ideal for businesses that need to track customer data over time, maintaining both current and historical records for customer profiles, contact details, and other relevant data.

**Data Warehousing and Analytics**:

The solution allows organizations to store large volumes of customer data in a Snowflake data warehouse, making it easier to perform analytics on both the current and historical state of customer information.

**Real-Time Data Ingestion and Processing**:

For applications that require real-time updates on customer data, this system enables instant ingestion, processing, and transformation of data, providing up-to-date information for business decisions.

**Historical Data Retention**:

The SCD Type 2 implementation ensures that changes in customer data are recorded over time, providing the ability to track the entire lifecycle of a customer's information and maintain a complete data history.

## Conclusion:
This project successfully implements an automated data pipeline to manage customer data, ensuring real-time ingestion, storage, and historical tracking through the use of industry-standard technologies such as Apache NiFi, Amazon S3, Snowflake, and Snowpipe. The system effectively addresses the challenge of tracking changes in customer data by utilizing Slowly Changing Dimensions (SCD) Type 1 and Type 2 methodologies, allowing both current and historical data to be maintained in an efficient and scalable manner. With automated tasks and real-time data monitoring, this project provides a seamless solution for handling and processing dynamic customer information.
