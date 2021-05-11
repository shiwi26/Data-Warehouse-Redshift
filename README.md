# Project Datawarehouse

## Project description

Sparkify is a music streaming startup with a growing user base and song database.

Their user activity and songs metadata data resides in json files in S3. The goal is to build an ETL pipeline that extracts their data from S3 buckets, puts them to the staging tables in Redshift, and transforms data into multiple dimensional tables and fact table in Redshift data warehouse. The purpose is to develope a data infrastructure for the analytics team to easily access and analyze the data.  

## Information needed

First is to fill the following information by creating an IAM role that has read access to S3, and save it as *dwh.cfg* in the project root folder, which will be used later in the create_cluster.ipynb file. 
         
```
[CLUSTER]
HOST=
DB_NAME=
DB_USER=
DB_PASSWORD=
DB_PORT=5439

[IAM_ROLE]
ARN=arn:

[S3]
LOG_DATA='s3://udacity-dend/log_data'
LOG_JSONPATH='s3://udacity-dend/log_json_path.json'
SONG_DATA='s3://udacity-dend/song_data'

[AWS]
KEY=
SECRET=

[DWH]
DWH_CLUSTER_TYPE       = multi-node
DWH_NUM_NODES          = 4
DWH_NODE_TYPE          = dc2.large
DWH_CLUSTER_IDENTIFIER = 
DWH_DB                 = 
DWH_DB_USER            = 
DWH_DB_PASSWORD        = 
DWH_PORT               = 5439
DWH_IAM_ROLE_NAME      = 
```

## Files

1. The create_cluster.ipynb is used to launch a redshift cluster using IAC.
2. The sql_queries.py file includes all the SQL statements with dropping and creating the tables, inserting the records and testing the number of records.
3. The create_tables.py file is used to run the creating and dropping statements in the sql_queries.py file by using '!python create_tables.py'.
4. The etl.py file is used to run the loading and inserting statements in the sql_queries.py file by using '!python etl.py'.
5. The Testing.py file is userd to run the testing statements in the sql_queries.py file by using '!python Testing.py'.


## Results

Number of rows in each table:

| Table            | rows  |
|---               | --:   |
| staging_events   | 8086  |
| staging_songs    | 14896 |
| songplay         | 333   |
| users            | 105   |
| songs            | 14896 |
| artists          | 10025 |
| time             | 8023  |


## Remarks: 
1. need to include a where statement to filter out null value in each insert statement, otherwise error will occur. 
2. user is a reserved key word, use users instead to create the table.