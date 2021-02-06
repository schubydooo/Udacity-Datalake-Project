###### This is apart of the Udacity Data Engineering Nanodegree program.
# Data Engineering Nanodegree - Datalake

## Overview 
This project is apart of the Udacity Data Engineering Nanodegree program and is the Datalake project.  Students are tasked with helping 'Sparkify' build their ETL solution to their datalake.  To do this, we built a pipeline to take JSON files stored in a S3 bucket, process the data in Spark and load the data onto a new S3 bucket in the parquet format.  


## Structure

1.  `etl.py` - Reads JSON files from S3, transforms data to desired analytics output using Spark, saves data to new S3 bucket in parquet format.

2.    `data/` - For local testing contains a subset of the data.  Change the input and output data paths in order to use this.


## Instructions

You will need to edit the configuration file  `dl.cfg`  and add your own AWS keys:

```
[AWS]
AWS_ACCESS_KEY_ID=<your_key>
AWS_SECRET_ACCESS_KEY=<your_key>
```
Additionally, you'll want to edit etl.py to point to the appropriate bucket.  To execute the ETL pipeline from the command line, enter the following:
```
python etl.py
```

## Analytics Schema

The analytics tables has the following fact table:

-   `songplays`  - log of user song plays

`songplays`  has foreign keys to the following dimension tables:

-   `users`
-   `songs`
-   `artists` 
-   `time` 