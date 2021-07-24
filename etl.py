import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import FloatType

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['KEYS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['KEYS']['AWS_SECRET_ACCESS_KEY']

def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

def process_song_data(spark, input_data, output_data):
    """
    Read song data from s3, create the songs_table and artists_table then load them back to s3.
    """
    # get filepath to song data file
    song_data = os.path.join(input_data, 'song_data', '*', '*', '*')
    
    # read song data file
    df = spark.read.json(song_data)
    df.createOrReplaceTempView("staging_songs")
    
    # extract columns to create songs table
    songs_table = spark.sql("""
        SELECT DISTINCT
            song_id,
            title,
            artist_id,
            year,
            duration
        FROM staging_songs
    """)
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(os.path.join(output_data, 'songs'), partitionBy=['year', 'artist_id'])

    # extract columns to create artists table
    artists_table = spark.sql("""
        SELECT DISTINCT
            artist_id,
            artist_name AS name,
            artist_location AS location,
            artist_latitude AS latitude,
            artist_longitude AS longitude
        FROM staging_songs
    """)
    
    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data, 'artists'))

def process_log_data(spark, input_data, output_data):
    """
    Read log data from s3, create the users table, songplays_table and time_table then load them back to s3.
    """
    # get filepath to log data file
    log_data = os.path.join(input_data, 'log_data', '*', '*')

    # read log data file
    df = spark.read.json(log_data)
    df.createOrReplaceTempView("staging_events")

    # filter by actions for song plays
    df =  spark.sql("""
        SELECT DISTINCT *
        FROM staging_events
        WHERE page = 'NextSong'
    """)

    # extract columns for users table    
    artists_table = spark.sql("""
        SELECT DISTINCT
            userId AS user_id,
            firstName AS first_name,
            lastName AS last_name,
            gender,
            level
        FROM staging_events
        WHERE userId IS NOT NULL
    """)
    
    # write users table to parquet files
    artists_table.write.parquet(os.path.join(output_data, 'users'))

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: x / 1000.0, FloatType())
    df = df.withColumn("timestamp", get_timestamp(df.ts))
    
    # create datetime column from original timestamp column
    df = df.withColumn("start_time", to_timestamp(col("ts") / 1000))
    
    # extract columns to create time table
    time_table = df.select('start_time')
    time_table = time_table.withColumn('hour', hour('start_time'))
    time_table = time_table.withColumn('day', dayofmonth('start_time'))
    time_table = time_table.withColumn('week', weekofyear('start_time'))
    time_table = time_table.withColumn('month', month('start_time'))
    time_table = time_table.withColumn('year', year('start_time'))
    time_table = time_table.withColumn('weekday', dayofweek('start_time'))
    
    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(os.path.join(output_data, 'time'), partitionBy=['year', 'month'])

    # read in song data to use for songplays table
    song_df = spark.read.json(os.path.join(input_data, 'song_data', '*', '*', '*'))

    # extract columns from joined song and log datasets to create songplays table 
    songplays_col = ["start_time", "userId as user_id", "level", "song_id", "artist_id", "sessionid as\
    session_id", "artist_location as location", "userAgent as user_agent"]
    
    songplays_table = songplays.selectExpr(songplays_col).dropDuplicates().dropna(subset=["user_id","artist_id", "song_id","start_time"]).withColumn("songplay_id",F.monotonically_increasing_id())

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(os.path.join(output_data, 'songplays'), partitionBy=['year', 'month'])

def main():
    """
    Extracts songs and events data from S3.Transform them into a set of fact and dimension tablesas using Spark, and load tables into s3 in parquet format.
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = ""
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)

if __name__ == "__main__":
    main()
