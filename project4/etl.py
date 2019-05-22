import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql import functions as F
from pyspark.sql import types as T
from datetime import datetime

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['KEY']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['SECRET']



def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = '{}/song_data/*/*/*/*.json'.format(input_data)

    # read song data file
    df = spark.read.json(song_data)
    
    # extract columns to create songs table
    songs_table = df.select('song_id','title','artist_id','year','duration')

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet("{}/songs_table".format(output_data),mode="overwrite")

    # extract columns to create artists table
    artists_table = df.select('artist_id','artist_name','artist_location',
                              'artist_latitude','artist_longitude')

    # write artists table to parquet files
    artists_table.write.parquet("{}/artists_table".format(output_data),mode="overwrite")
    
    
def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = '{}/log_data/*.json'.format(input_data)

    # read log data file
    df = spark.read.json(log_data)

    # filter by actions for song plays
    df = df.where(col("page").isin({"NextSong"}))

    # extract columns for users table
    users_table = df.select('userId', 'firstName', 'lastName', 'gender', 'level')

    # write users table to parquet files
    users_table.write.parquet("{}/users_table".format(output_data),mode="overwrite")
    
    # turn milli into seconds epoch
    df = df.withColumn('ts', col('ts')/1000)
    
    #convert epoch to text
    df = df.withColumn('timestamp', F.date_format(df.ts.cast(dataType=T.TimestampType()), 
                                                  "yyyy-MM-dd hh:mm:ss"))
    df = df.withColumn('datetime', F.date_format(df.ts.cast(dataType=T.TimestampType()), 
                                                 "yyyy-MM-dd"))
    #convert text to timestamp and datatype
    df = df.withColumn('timestamp', F.to_date(df.timestamp.cast(dataType=T.TimestampType())))
    df = df.withColumn('datetime', F.to_date(df.datetime.cast(dataType=T.DateType())))
    df = df.withColumn('hour', F.hour(df.timestamp))
    df = df.withColumn('day', F.dayofyear(df.datetime))
    df = df.withColumn('week', F.weekofyear(df.datetime))
    df = df.withColumn('month', F.month(df.datetime))
    df = df.withColumn('year', F.year(df.datetime))
    df = df.withColumn('weekday', F.when((F.dayofweek(df.datetime) == 0) | 
                                         (F.dayofweek(df.datetime) == 1) | 
                                         (F.dayofweek(df.datetime) == 7), 0).otherwise(1))
    
    # extract columns to create time table
    time_table = df.select('ts', 'datetime','hour','day','week','month','year','weekday')

    # write time table to parquet files partitioned by year and month
    time_table.write.parquet("{}/time_table".format(output_data),mode="overwrite")

    # read in song data to use for songplays table
    songoutput = '{}/songs_table/*'.format(output_data)
    song_df = spark.read.parquet("s3a://dend-project4/songs_table/*")
    ldf = df.alias('ldf')
    sdf = song_df.alias('sdf')
    left_join_log_song = ldf.join(sdf, ldf.song == sdf.title, how='left')

    # extract columns from joined song and log datasets to create songplays table
    songplays_table = left_join_log_song.select('timestamp', 'userId', 'level', 'song_id', 
                                                'artist_id', 'sessionId', 'location', 'userAgent')

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet("{}/songplays_table".format(output_data),mode="overwrite")
    

def main():
    spark = create_spark_session()
    input_data = config['S3']['IN_PATH']
    output_data = config['S3']['OUT_PATH']

    # uncomment for local files
    # input_data = 'input'
    # output_data = 'output'

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)

if __name__ == "__main__":
    main()
