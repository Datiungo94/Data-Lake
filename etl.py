import configparser
from datetime import datetime
import os
from pyspark.sql.types import TimestampType, DateType
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    This function create a Spark Session which is the entry point of programming Spark.
    Args: None
    
    Returns: Spark
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark,input_data, output_data):
    """
    This function processes song data from a S3 bucket and extracts 2 tables in parquet format - songs and artists
    in another S3 bucket specified in 'output_data'
    Args:
        - spark: A Spark Session
        - input_data: S3 link to Song data
        - output_data: S3 link to drop extracted tables
    Returns: None
    """
    # read song data file
    df = spark.read.json(input_data)

    # extract columns to create songs table
    songs_table = df.select('song_id', 'title', 'artist_id', 'year', 'duration')
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode('overwrite').parquet(output_data + "/song_data/")

    # extract columns to create artists table
    artists_table = df.select('artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude')
    
    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(output_data + '/artists/')


def process_log_data(spark, input_data, output_data):
    """
    This function process log data and extracts 3 tables in parquet format - users, time, and songplay.
    The timestamp in log data is broken down into hour, day, week, month, year, and weekday.
    Args:
        - spark: A Spark Session
        - input_data: S3 link to Log data
        - output_data: S3 link to drop extracted tables and grab the songs data that was created in func 'process_song_data'
    Returns: None
    """
    # read log data file
    log_df = spark.read.json(input_data)
    
    # filter by actions for song plays
    log_filtered_df = log_df.where(log_df.page == "NextSong")

    # extract columns for users table    
    users_table = log_filtered_df.select('userId', 'firstName', 'lastName', 'gender', 'level').dropDuplicates(['userId'])
    
    # write users table to parquet files
    users_table.write.mode('overwrite').parquet(output_data + '/users/')

    # create timestamp column from original timestamp column
    # Columns ts is in milliseconds. Divide by 1000 to get the results in seconds and convert to Timestamp Type.
    log_filtered_df = log_filtered_df.withColumn('tsconvert', (col('ts') /1000).cast(TimestampType()))
    log_filtered_df.createOrReplaceTempView("log_staging")
    
    # extract columns to create time table
    time_table = log_filtered_df.select(col('tsconvert').alias('start_time'),
                              hour('tsconvert').alias('hour'),
                              dayofmonth('tsconvert').alias('day'),
                              weekofyear('tsconvert').alias('week'),
                              month('tsconvert').alias('month'),
                              year('tsconvert').alias('year'),
                              date_format('tsconvert', 'EEEE').alias('weekday'))
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year', 'month').mode('overwrite').parquet(output_data + '/time/')

    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data + "/song_data/*.parquet")
    song_df.createOrReplaceTempView("songs_staging")

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql("""SELECT logs.tsconvert AS starttime, 
                                          logs.userId, 
                                          logs.level, 
                                          songs.song_id, 
                                          songs.artist_id, 
                                          logs.sessionId, logs.location, 
                                          logs.userAgent, 
                                          year(logs.tsconvert) as year, 
                                          month(logs.tsconvert) as month
        FROM songs_staging AS songs 
        INNER JOIN log_staging AS logs ON logs.song = songs.title""")

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year', 'month').mode('overwrite').parquet(output_data + '/songplays/')


def main():
    spark = create_spark_session()

    song_input_data = "s3://udacity-dend/song_data/*/*/*/*.son"
    log_input_data = "s3://udacity-dend/log_data/*/*/*.json"
    
    output_data = "s3://sparkify-project-songs"
    
    process_song_data(spark, song_input_data, output_data)    
    process_log_data(spark, log_input_data, output_data)


if __name__ == "__main__":
    main()
