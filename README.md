# Work Versioning:

Below the main modifications:
* V0: This is the first submission


# 1. Project summary:
## 1.1 The Business problem:

The startup `Sparkify` has grown their user base and song database and want **to move their processes and data onto a Data Lake**. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

## 1.2 Inputs:

The folowing folders were given at the beginning of the project:

* Song data: s3://udacity-dend/song_data (with JSON songs data files)
* Log data: s3://udacity-dend/log_data (with JSON log data files)


# 2. The project output:
## 2.1 the ETL processing in the cloud:

As we were interested in having aggregated results easily, the **star schema** were advised and implemented. 

The star schema is made up of the following tables:

* Dimension tables (with features columns within parenthesis):
    1. users (columns list: user_id, first_name, last_name, gender, level)
    2. songs (columns list: song_id, title, artist_id, year, duration)
    3. artists (columns list: artist_id, name, location, latitude, longitude)
    4. time (columns list: start_time, hour, day, week, month, year, weekday)

* Fact table: 
    1. songplays (columns list: songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)

This project aim is to build the ETL pipeline by performing the following actions:

* `etl.py` reads songs and logs data from S3, process that data using Spark and writes them back to S3.

*I found that an optimization is to read the data from S3, process the data using Spark, writes the parquet files in hadoop HDFS cluster and copy the files back to S3 at end. With the optimized process, should be much faster.*

## 2.2 How the use the files:


1. Create a cluster on Amazon EMR
2. SSH log-in into the master node
3. copy the `etl.py` file into S3 and then into hadoop hdfs cluster
4. run spark-submit

Below a summup of the ETL process:

inputs                     | Extraction  | Load data
----------------------------------------------------------------------------------
songs & logs files | `etl.py`        | EMR
----------------------------------------------------------------------------------
