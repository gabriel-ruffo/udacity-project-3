import configparser
import time
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, from_unixtime
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, LongType, TimestampType, MapType

song_schema = StructType([
    StructField("num_songs", IntegerType(), True),
    StructField("artist_id", StringType(), True),
    StructField("artist_latitude", FloatType(), True),
    StructField("artist_longitude", FloatType(), True),
    StructField("artist_location", StringType(), True),
    StructField("artist_name", StringType(), True),
    StructField("song_id", StringType(), True),
    StructField("title", StringType(), True),
    StructField("duration", FloatType(), True),
    StructField("year", IntegerType(), True)
])

log_schema = StructType([
    StructField("artist", StringType(), True),
    StructField("auth", StringType(), True),
    StructField("firstName", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("itemInSession", IntegerType(), True),
    StructField("lastName", StringType(), True),
    StructField("length", FloatType(), True),
    StructField("level", StringType(), True),
    StructField("location", StringType(), True),
    StructField("method", StringType(), True),
    StructField("page", StringType(), True),
    StructField("registration", LongType(), True),
    StructField("sessionId", IntegerType(), True),
    StructField("song", StringType(), True),
    StructField("status", IntegerType(), True),
    StructField("ts", LongType(), True),
    StructField("userAgent", StringType(), True),
    StructField("userId", StringType(), True)
])

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID'][1:-1]
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY'][1:-1]

def create_spark_session():
    """
    Initiates a Spark session using Hadoop version 2.7.0.
    Parameters: None
    Returns: A Spark session
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Processes the raw json files containing song data. It
        reads in the files, frames them with the desired
        columns, and writes them out as parquet files to
        S3.
    Parameters: spark: Spark session
                input_data: filepath to json files
                output_data: filepath for parquet files
    Returns: None
    """
    # get filepath to song data file
    song_data = input_data + "song-data/*/*/*/*.json"
    
    # read song data file
    df = spark.read.json(song_data, schema=song_schema)
    
    # extract columns to create songs table
    dfSongs = df.createOrReplaceTempView("songs")
    songs_table = spark.sql("""
                    SELECT song_id, title, artist_id, year, duration
                    FROM songs""")
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(output_data + "songs", mode = "overwrite", partitionBy = ('year', 'artist_id'))

    # extract columns to create artists table
    dfArtists = df.createOrReplaceTempView("artists")
    artists_table = spark.sql("""
                        SELECT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
                        FROM artists""")
    
    # write artists table to parquet files
    artists_table.write.parquet(output_data + "artists", mode = "overwrite")

@udf(MapType(StringType(), StringType()))
def parseDateUDF(line):
    """
    UDF function to extract time information from raw
        UTC line.
    Parameters: line: raw encoded time information
    Returns: Dictionary containing the desired time fields
    """
    hour = line.hour
    day = line.day
    week = datetime.date(line).isocalendar()[1]
    month = line.month
    year = line.year
    weekday = line.weekday()
    
    return {
        "start_time" : str(line),
        "hour" : hour,
        "day" : day,
        "week" : week,
        "month" : month,
        "year" : year,
        "weekday" : weekday
    }
     

def process_log_data(spark, input_data, output_data):
    """
    Processes the raw json files containing log data. It
        reads in the files, frames them with the desired
        columns, and writes them out as parquet files to
        S3.
    Parameters: spark: Spark session
                input_data: filepath to json files
                output_data: filepath for parquet files
    Returns: None
    """
    # get filepath to log data file
    log_data = input_data + "log-data/*/*/*.json"

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter("page = 'NextSong'")

    # extract columns for users table
    dfUsers = df.createOrReplaceTempView("users")
    artists_table = spark.sql("""
                        SELECT userId, firstName, lastName, gender, level
                        FROM users""")
    
    # write users table to parquet files
    artists_table.write.parquet(output_data + "users", mode = "overwrite")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp((x / 1000.0)), TimestampType())
    df = df.withColumn("timestamp", get_timestamp(df.ts))
    
    # create datetime column from original timestamp column
    df = df.withColumn("datetime", parseDateUDF(df.timestamp))
    
    # extract columns to create time table
    time_df = df.select("datetime")
    fields = ["start_time", "hour", "day", "week", "month", "year", "weekday"]
    exprs = ["datetime['{}'] as {}".format(field, field) for field in fields]
    time_df = time_df.selectExpr(*exprs)
    
    # write time table to parquet files partitioned by year and month
    time_df.write.parquet(output_data + "time", mode = "overwrite", partitionBy = ("year", "month"))

    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data + "songs")
    song_df = song_df.withColumnRenamed("year", "song_year")

    # extract columns from joined song and log datasets to create songplays table 
    conds = [song_df.title == df.song, song_df.duration == df.length]

    songplays_table = song_df.join(df, conds).where(df.page == 'NextSong')
    songplays_table = songplays_table.join(time_df, songplays_table.timestamp == time_df.start_time)
    
    songplays_table.createOrReplaceTempView("songplays")
    songplays_table_filtered = spark.sql("""
                                    SELECT datetime as start_time, userId, level, song_id, artist_id, 
                                        sessionId, location, userAgent, year, month
                                    FROM songplays""")
    songplays_table_filtered = songplays_table_filtered.withColumn("songplay_id", monotonically_increasing_id())
    songplays_table_filtered.show(10, truncate = False)

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(output_data + "songplays", mode = "overwrite", partitionBy = ("year", "month"))


def main():
    """
    Initiates the spark session and makes calls to 
        process the song and log data.
    Parameters: None
    Returns: None
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://sparkify-parquet-files/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
