from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import TimestampType, IntegerType
from pyspark.sql.functions import udf, col, row_number
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
# Additionnal libraries
from pyspark.sql import Window

# Getting AWS credentials

os.environ['AWS_ACCESS_KEY_ID']='' # to complete
os.environ['AWS_SECRET_ACCESS_KEY']='' # to complete

# function definitions:
def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Processing log_data 
        
    Processing song_data from S3 to local directory. 
    Creates song and artists dimension tables
    
    Args:
        spark: SparkSession
        input_data: Root-URL to S3 bucket 
        output_data: None
    Returns:
        None
    """
    # get filepath to song data file
    song_data = input_data + "song_data/*/*/*/*.json" # to modify for AWS "song_data/*/*/*/*.json"
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select('song_id', 'title', 'artist_id', 'year', 'duration').dropDuplicates()
    
    # write songs table to parquet files partitioned by year and artist
    print('Writing songs table')
    # V1: songs_table.write.mode("overwrite").partitionBy('year', 'artist_id').parquet(output_data + 'parquet-songs/')
    time_table.write.parquet(os.path.join(output_data, "time_table/"), mode='overwrite', partitionBy=["year","month"])

    # extract columns to create artists table
    artists_table = df.select('artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude').dropDuplicates()
    
    # write artists table to parquet files
    print('Writing artist table')
    # V1: artists_table.write.mode('overwrite').parquet(output_data + 'parquet-artists/')
    artists_table.write.parquet(os.path.join(output_data, "parquet-artists/"), mode='overwrite)
    #
    print('Done processing songs!')


def process_log_data(spark, input_data, output_data):
    """
    Processing log_data 
        
    Processing log_data from S3 to local directory. 
    Creates users, time dimension tables and songplays fact table
    
    Args:
        spark: SparkSession
        input_data: Root-URL to S3 bucket 
        output_data: None
    Returns:
        None
    """                            
    # get filepath to log data file
    log_data = input_data + "log_data/*/*/*.json" # to modify on AWS: "log_data/*/*/*.json"

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter("page='NextSong'")

    # extract columns for users table    
    users_table = df.select('userId', 'firstName', 'lastName', 'gender', 'level').dropDuplicates()\
                                                                             .withColumnRenamed('userId', 'user_id')\
                                                                             .withColumnRenamed('lastName', 'last_name')\
                                                                             .withColumnRenamed('firstName', 'first_name')
    
    # write users table to parquet files
    print('Writing users table')
    # V1: users_table.write.mode('overwrite').parquet(output_data + 'parquet-users/')
    users_table.write.parquet(os.path.join(output_data, "parquet-users/"), mode='overwrite)

    # create timestamp function from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x/1000), TimestampType())
    
    # create datetime column from original timestamp column
    df = df.withColumn('start_time', get_timestamp('ts'))
    
    # extract columns to create time table
    time_table = df.select('start_time', hour('start_time').alias('hour'), dayofmonth('start_time').alias('day'),
                      weekofyear('start_time').alias('week'), month('start_time').alias('month'),
                      year('start_time').alias('year'), 
                      date_format(col('start_time'), 'D').cast(IntegerType()).alias('weekday')).dropDuplicates()
    
    # write time table to parquet files partitioned by year and month
    print('Writing time table')
    # V1: time_table.write.mode('overwrite').partitionBy('year', 'month').parquet(output_data + 'parquet-time/')
    time_table.write.parquet(os.path.join(output_data, "parquet-time/"), mode='overwrite, partitionBy('year' ,'month'))

    # read in song data to use for songplays table
    song_table = spark.read.parquet(os.path.join(output_data, 'parquet-songs/'))

    # extract columns from joined song and log datasets to create songplays table
    ## Create songplays_table:
    windowSpec  = Window.orderBy("start_time")
    songplays_table = df.join(song_table, df['song']==song_table['title'], 'inner')\
                        .select('start_time', 'userId', 'level', 'song_id', 'artist_id', 'sessionId', 'location', 'userAgent')\
                        .withColumn('songplay_id', row_number().over(windowSpec))\
                        .withColumnRenamed('userId', 'user_id')\
                        .withColumnRenamed('sessionId', 'session_id')\
                        .withColumnRenamed('userAgent', 'user_agent')\
                        .dropDuplicates()
    ## reordering columns:
    songplays_table = songplays_table.select('songplay_id', 'start_time', 'user_id', 'level', 'song_id', 'artist_id', 'session_id', 'location', 'user_agent')

    # write songplays table to parquet files partitioned by year and month
    print('Writing songplays table')
    songplays_table.join(time_table, 'start_time', 'inner')\
                    .select('songplay_id', 'start_time', 'user_id', 'level', 'song_id', 'artist_id', 'session_id','location', 'user_agent', 'year', 'month')\
                    .write.mode('overwrite').partitionBy('year', 'month')\
                    .parquet(os.path.join(output_data,'parquet-songplays/'))
    #
    print('Done processing logs !')


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "hdfs:///user/sparkify_data/output/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()