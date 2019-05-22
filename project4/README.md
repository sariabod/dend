Purpose of Project
------------------

Create an ETL pipeline to parse song and log data and store them into s3 so they can be later queried by stake holders and analysts.

Steps:

1. Update dl.cfg with AWS info: Credentials, In Bucket, Out Bucket
2. Run etl.py 
3. When etl is run data is loaded from s3 buckets into Pyspark Dataframes.
4. The appropriate fields from the dataframes are selected, corrected, then saved back to s3 as parquet files.
* The timestamps were converted to text then to Timestamp and DateTime fields as they performed better then udf() for this specific task








