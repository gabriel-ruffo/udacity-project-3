DESCRIPTION:
The process for this project is similar to that of past ETL processes.
Data stored in S3 buckets for song and log information is retreived using
AWS credentials and momentarily stored in Data Frames. Using pyspark,
the data is transformed and framed into the star schema given, and rewritten
to S3 as parquet files. These files represent the different tables, and some
are partitioned respectively. 

FILES:
etl.py: Contains the ETL logic.
dl.cfg: Contains the AWS credentials for pulling from and pushing to S3.
README.md: Explanation of files and processes.