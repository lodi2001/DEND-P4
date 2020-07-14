import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import* #udf, col ,unix_timestamp
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import*

config = configparser.ConfigParser()
config.read('dl.cfg')
print('config')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = input_data + "song_data/A/A/A/*.json"
    
    # read song data file
    df = spark.read.json(song_data)
    df.createOrReplaceTempView("song_data")
    # extract columns to create songs table
    songs_table = spark.sql("""
    SELECT DISTINCT 
        song_id,
          title,
          artist_id,
          year,
          duration
    FROM song_data
    """)
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode('overwrite').partitionBy("year", "artist_id").parquet(output_data + 'songs_table/')

    # extract columns to create artists table
    artists_table = spark.sql("""
    SELECT DISTINCT 
        artist_id,
          artist_name as name,
          artist_location as location,
          artist_latitude as latitude,
          artist_longitude as longitude 
    FROM song_data
    """)
    
    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(output_data + 'artists_table/')


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data =input_data + "/log_data/*/*/"

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df =  df.where('page = "NextSong"')
    df.createOrReplaceTempView("log_data")
    # extract columns for users table    
    users_table = spark.sql("""
    SELECT DISTINCT 
        userId as user_id,
          firstName as first_name,
          lastName as last_name,
          gender,
          level    
    FROM log_data
    """)
    
    # write users table to parquet files
    users_table.write.mode('overwrite').parquet(output_data + 'users_table/')

    # create timestamp column from original timestamp column
    def format_datetime(ts):
        return datetime.fromtimestamp(ts/1000.0)
    get_timestamp = udf(lambda x: format_datetime(int(x)),TimestampType())
    df = df.withColumn("timestamp", get_timestamp(df.ts))
        
    #get_timestamp = udf(lambda x: x/1000, IntegerType())
   # df = df.withColumn('start_time', get_timestamp('ts'))
    
    # create datetime column from original timestamp column
    
    get_datetime = udf(lambda x:format_datetime(int(x)),DateType())
    df = df.withColumn("datetime", get_timestamp(df.ts))
    
   # get_datetime = udf(lambda x: from_unixtime(x), TimestampType())
   # df = df.withColumn('datetime', from_unixtime('start_time'))
  #  df.createOrReplaceTempView("log_data")
    
    time_table = df.select('ts','datetime','timestamp',year(df.datetime).alias('year'), month(df.datetime).alias('month')).dropDuplicates()
    
    # extract columns to create time table
 #  time_table = spark.sql("""
  #  SELECT DISTINCT 
   #     ts as start_time,
   #       timestamp,
    #      cast(date_format(timestamp, "HH") as INTEGER) as hour,
     #     cast(date_format(timestamp, "dd") as INTEGER) as day,
     #     weekofyear(timestamp) as week ,
      #    month(timestamp) as month,
       #   year(timestamp) as year,
        #  dayofweek(timestamp) as weekday,
   # FROM log_data
  #  """)
  
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode('overwrite').partitionBy(
        "year", "month").parquet(output_data + 'time_table/')
    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data+'songs_table/')
    
    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql("""
    SELECT DISTINCT 
        ts as start_time,
          month(timestamp) as month,
          year(timestamp) as year,
          ld.userId as user_id,
          ld.level,
          sd.song_id,
          sd.artist_id,
          ld.sessionId as session_id,
          ld.location,
          ld.userAgent as user_agent
    FROM log_data ld
    JOIN song_data sd ON 
        ld.artist = sd.artist_name
        and ld.song = sd.title
        and ld.length = sd.duration
    """)

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode('overwrite').partitionBy(
        "year", "month").parquet(output_data + 'songplays_table/')


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://dend-p04/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
