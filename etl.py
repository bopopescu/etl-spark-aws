#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Process data from logs and songs files stores in S3 bucket and save the data
transformed back to another S3 bucket

@author: udacity, ucaiado

Created on 05/06/2020
"""

import argparse
import textwrap
from datetime import datetime
import time
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, row_number
from pyspark.sql.types import DateType
from pyspark.sql.window import Window
from pyspark.sql.functions import (year, month, dayofmonth, hour, weekofyear,
                                   date_format, dayofweek)

'''
Begin help functions and variables
'''


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


'''
End help functions and variables
'''


def process_song_data(spark, input_data, output_data):
    '''
    Porcess data from songs files

    :param spark: Spark session object.
    :param input_data: string. S3 path data to transform
    :param output_data: string. S3 root path to store transformed data
    '''

    # get filepath to song data file
    song_data = input_data  # shoud be already in S3 format

    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df['song_id', 'title', 'artist_id', 'year', 'duration']

    # write songs table to parquet files partitioned by year and artist
    s_s3_path = "{}/songs.parquet".format(output_data)
    songs_table.write.mode('overwrite').partitionBy(
        'year', 'artist_id').parquet(s_s3_path)

    # extract columns to create artists table
    artists_table = df.select(
        col("artist_id").alias("artist_id"),
        col("artist_name").alias("name"),
        col("artist_location").alias("location"),
        col("artist_latitude").alias("latitude"),
        col("artist_longitude").alias("longitude"))
    artists_table = artists_table.drop_duplicates(subset=['artist_id'])

    # write artists table to parquet files
    s_s3_path = "{}/artists.parquet".format(output_data)
    artists_table.write.mode('overwrite').parquet(s_s3_path)


def process_log_data(spark, input_data, output_data):
    '''
    Porcess data from log files

    :param spark: Spark session object.
    :param input_data: string. S3 path data to transform
    :param output_data: string. S3 root path to store transformed data
    '''

    # get filepath to log data file
    log_data = input_data

    # read log data file
    df = spark.read.json(log_data)

    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table
    users_table = df.select(
        col("userId").alias("user_id"),
        col("firstName").alias("first_name"),
        col("lastName").alias("last_name"),
        col("gender").alias("gender"),
        col("level").alias("level"))
    users_table = users_table.drop_duplicates(subset=['user_id'])

    # write users table to parquet files
    s_s3_path = "{}/users.parquet".format(output_data)
    users_table.write.mode('overwrite').parquet(s_s3_path)

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda ts: ts/1000)
    df = df.withColumn('timestamp', get_timestamp('ts'))

    # create datetime column from original timestamp column
    get_datetime = udf(lambda ts: datetime.fromtimestamp(ts/1000), DateType())
    df = df.withColumn('datetime', get_datetime('ts'))

    # extract columns to create time table
    time_table = df.select(
        col('datetime').alias('start_time'),
        hour('datetime').alias('hour'),
        dayofmonth('datetime').alias('day'),
        weekofyear('datetime').alias('week'),
        month('datetime').alias('month'),
        year('datetime').alias('year'),
        dayofweek('datetime').alias('weekday')
    )
    time_table = time_table.drop_duplicates(subset=['start_time'])

    # write time table to parquet files partitioned by year and month
    s_s3_path = "{}/time.parquet".format(output_data)
    time_table.write.mode('overwrite').partitionBy(
        'year', 'month').parquet(s_s3_path)

    # read in song data to use for songplays table
    s_s3_path = "{}/songs.parquet".format(output_data)
    songs_table = spark.read.parquet(s_s3_path)

    # extract columns from joined song and log datasets to create songplays table
    window = Window.orderBy(col('song_id'))

    songplays_table = (df.join(
        songs_table.alias('songs'), df.song == col('songs.title'))
         .select(
            col('datetime').alias('start_time'),
            month('datetime').alias('month'),
            year('datetime').alias('year'),
            col('userId').alias('user_id'),
            col('level').alias('level'),
            col('songs.song_id').alias('song_id'),
            col('songs.artist_id').alias('artist_id'),
            col('sessionId').alias('session_id'),
            col('location').alias('location'),
            col('userAgent').alias('user_agent'),
         )
         .withColumn('songplay_id', row_number().over(window))
    )

    # write songplays table to parquet files partitioned by year and month
    s_s3_path = "{}/songplays.parquet".format(output_data)
    songplays_table.write.mode('overwrite').partitionBy(
        'year', 'month').parquet(s_s3_path)


def main(s_input_songs, s_input_logs, s_output):
    '''
    Load data into stanging and analytical tables

    :param s_input_songs: string. S3 path to songs data to transform
    :param s_input_logs: string. S3 path to logs data to transform
    :param s_output: string. S3 root path to store transformed data
    '''

    spark = create_spark_session()
    input_song_data = s_input_songs
    input_logs_data = s_input_logs
    output_data = s_output

    process_song_data(spark, input_song_data, output_data)
    process_log_data(spark, input_logs_data, output_data)


if __name__ == "__main__":
    s_txt = '''\
            Extract, transform and load data to Data Lake in S3
            --------------------------------
            ..
            '''
    # include and parse variables
    obj_formatter = argparse.RawDescriptionHelpFormatter
    parser = argparse.ArgumentParser(
        formatter_class=obj_formatter, description=textwrap.dedent(s_txt))

    s_help = 'input S3 path to song data'
    parser.add_argument('-s', '--songs', default=None, type=str, help=s_help)

    s_help = 'input S3 path to log data'
    parser.add_argument('-l', '--logs', default=None, type=str, help=s_help)

    s_help = 'output S3 path'
    parser.add_argument('-o', '--output', default=None, type=str, help=s_help)

    # recover arguments
    args = parser.parse_args()
    s_input_songs = args.songs
    s_input_logs = args.logs
    s_output = args.output

    s_err = 'Enter an S3 path to both input and output files'
    assert s_output and s_input_songs and s_input_logs, s_err

    # process data
    main(s_input_songs, s_input_logs, s_output)
