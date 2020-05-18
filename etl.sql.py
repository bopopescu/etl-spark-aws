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
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, row_number
from pyspark.sql.types import DateType
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
    df.createOrReplaceTempView("staging_songs")
    songs_table = spark.sql("""
        SELECT song_id, title, artist_id, year, duration
        FROM staging_songs
        ORDER BY song_id
    """)

    # write songs table to parquet files partitioned by year and artist
    s_s3_path = "s3://{}/songs.parquet".format(output_data)
    songs_table.write.mode('overwrite').partitionBy(
        'year', 'artist_id').parquet(s_s3_path)

    # extract columns to create artists table
    artists_table = sqlContext.sql("""
        SELECT (
            artist_id,
            artist_name AS name,
            artist_location AS location,
            artist_latitude AS latitude,
            artist_longitude AS longitude)
        FROM staging_songs
        ORDER BY artist_id
    """)

    # write artists table to parquet files
    s_s3_path = "s3://{}/artists.parquet".format(output_data)
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
    df.createOrReplaceTempView("staging_events")
    df = spark.sql("""
        SELECT * FROM staging_events
        WHERE page='NextSong'
        """)

    # extract columns for users table
    users_table = sqlContext.sql("""
        SELECT DISTINCT
            userId AS user_id,
            firstName AS first_name,
            lastName AS last_name,
            gender AS gender,
            level AS level
        FROM staging_events
        ORDER BY user_id
    """)

    # write users table to parquet files
    s_s3_path = "s3://{}/users.parquet".format(output_data)
    users_table.write.mode('overwrite').parquet(s_s3_path)

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: time.localtime(x/1000.))
    df = df.withColumn('timestamp', get_timestamp('ts'))

    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: time.strftime(
        '%Y-%m-%d %H:%M:%S', time.localtime(x/1000.0)), DateType())
    df = df.withColumn('datetime', get_datetime('ts'))

    # extract columns to create time table
    time_table = spark.sql("""
        SELECT DISTINCT
            datetime AS start_time,
            hour(timestamp) AS hour,
            dayofmonth(timestamp) AS day,
            weekofyear(timestamp) AS week,
            month(timestamp) AS month,
            year(timestamp) AS year,
            dayofweek(timestamp) AS weekday
        FROM staging_events
        ORDER BY start_time
    """)

    # write time table to parquet files partitioned by year and month
    s_s3_path = "s3://{}/time.parquet".format(output_data)
    time_table.partitionBy(
        'year', 'month').write.mode('overwrite').parquet(s_s3_path)

    # read in song data to use for songplays table
    s_s3_path = "s3://{}/songs.parquet".format(output_data)
    songs_table = spark.read.parquet(s_s3_path)

    # extract columns from joined song and log datasets to create songplays table
    songplays_table = spark.sql("""
        SELECT DISTINCT
            events.timestamp AS start_time,
            events.userId AS user_id,
            events.level AS level,
            songs.song_id AS song_id,
            songs.artist_id AS artist_id,
            events.sessionId AS session_id,
            events.location AS location,
            events.userAgent AS user_agent
        FROM staging_events AS events
        INNER JOIN songs_table AS songs
            ON events.song = songs.title
            AND events.length = songs.duration
        ORDER BY song_id
    """)

    window = Window.orderBy(col('song_id'))
    songplays_table = songplays_table.withColumn(
        'songplay_id', row_number().over(window))

    # write songplays table to parquet files
    s_s3_path = "s3://{}/songplays.parquet".format(output_data)
    songplays_table.write.mode('overwrite').parquet(s_s3_path)


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
