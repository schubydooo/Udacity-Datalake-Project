import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import TimestampType
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']

def create_spark_session():
    '''Create an Apache Spark session to process the data

    Returns:
    Apache Spark session
    '''
    spark = SparkSession \
        .builder \
        .appName('CreateSparkifyDatalake') \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    '''Take the input song data, transform it to end-state, and write to S3 using Parquet file format'''

    # get filepath to song data file
    song_data = f"{input_data}song_data/*/*/*/*.json"
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select('song_id', 'title', 'artist_id', 'year', 'duration')

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy(['year', 'artist_id']).parquet(f"{output_data}/songs.parquet", mode='overwrite')

    # extract columns to create artists table
    artists_table = df.selectExpr('artist_id', 
        'artist_name as name', 
        'artist_location as location', 
        'artist_latitude as latitude', 
        'artist_longitude as longitude',
        )    
    # write artists table to parquet files
    artists_table.write.parquet(f'{output_data}/artists.parquet', mode='overwrite')


def process_log_data(spark, input_data, output_data):
    '''Take the input user log data, transform it to end-state, and write to S3 in Parquet file format'''

    # get filepath to log data file
    log_data = f'{input_data}log-data/*/*/*.json'

    # read log data file
    df = spark.read.json(log_data)

    # filter by actions for song plays
    df = df.filter('page = "NextSong"')

    # extract columns for users table    
    artists_table = df.selectExpr('userId as user_id', 'firstName as first_name', 'lastName as last_name', 'gender', 'level').distinct()

    # write users table to parquet files
    artists_table.write.parquet(f'{output_data}/users.parquet', mode='overwrite')

    get_timestamp = udf(lambda x: datetime.fromtimestamp(x/1000), TimestampType())
    df = df.withColumn('start_time', get_timestamp('ts'))

    # create timestamp column from original timestamp column
    df.createOrReplaceTempView("log_data") 
    time_table = spark.sql('\
        select t.start_time \
        ,hour(t.start_time) as hour \
        ,day(t.start_time) as day \
        ,weekofyear(t.start_time) as week \
        ,month(t.start_time) as month \
        ,year(t.start_time) as year \
        ,dayofweek(t.start_time) as weekday\
        from \
            (select start_time \
            from log_data \
            group by start_time \
            ) t'
    )

    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year', 'month').parquet(f'{output_data}/time.parquet', mode='overwrite')

    # read in song data to use for songplays table
    song_df = spark.read.parquet(f'{output_data}/songs.parquet')

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.join(song_df, song_df.title == df.song, how='left')
    songplays_table = songplays_table.selectExpr('monotonically_increasing_id() as songplay_id','start_time', 'userId as user_id', 'level', 'song_id', 'artist_id', 'sessionId as session_id', 'location', 'userAgent as user_agent')

    # write songplays table to parquet files partitioned by year and month
    songplays_table = songplays_table.withColumn('year', year(songplays_table.start_time))\
        .withColumn('month', month(songplays_table.start_time))
    songplays_table.write.partitionBy('year', 'month').parquet(f'{output_data}/songplays.parquet')


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-dend-project-4-schubert/"
    # input_data = "data/"
    # output_data = "output/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
