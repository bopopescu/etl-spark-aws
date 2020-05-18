# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:light
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.3.0
#   kernelspec:
#     display_name: zipline
#     language: python
#     name: zipline
# ---

# ## Startup Spark locally

# +
# import findspark
# findspark.init()

# import pyspark
# sc = pyspark.SparkContext(appName="myAppName")


# +
# from pyspark.sql import SQLContext
# sqlContext = SQLContext(sc)
# -

from pyspark.sql import SparkSession
from pyspark.sql.functions import isnan, count, when, col, desc, udf, col, sort_array, asc, avg
from pyspark.sql.functions import sum as Fsum
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType

spark = SparkSession \
    .builder \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
    .getOrCreate()


# ## Load song data
#
# For some reason, we can not load all the data at once. the function bellow
#   read the 30 files at once in each step

def load_data(l_files):
    # NOTE: for some reason, locally I could not load more than 30 files by step
    i_step = 30
    df = None
    for ii in range(0, len(l_files), i_step):
        if ii == 0:
            continue
        df_new = spark.read.json(l_files[ii-i_step:ii])
        if isinstance(df, type(None)):
            df = df_new
        else:
            df = df.union(df_new)
    return df


data = './data/songs/A/*/*.json'
output_data = './data/output'
l_files = !ls {data}
l_files2 = list(l_files)
print(f'total fies: {len(l_files)}')

# +
# df = spark.read.json(l_files2)
# df.printSchema()
# -

# %%time
df = load_data(l_files2)
df.printSchema()

# ## extract columns to create songs table

# +
# songs - songs in music database
# song_id, title, artist_id, year, duration

df.createOrReplaceTempView("staging_songs")
songs_table = spark.sql("""
    SELECT song_id, title, artist_id, year, duration
    FROM staging_songs
    ORDER BY song_id
""")

songs_table.printSchema()
# -

songs_table = df['song_id', 'title', 'artist_id', 'year', 'duration']
songs_table.printSchema()

songs_table.limit(1).show()

# write songs table to parquet files partitioned by year and artist
s_song_s3_path = f"s3://{output_data}/songs.parquet"
s_song_s3_path = f"{output_data}/songs.parquet"
songs_table.write.mode('overwrite').partitionBy('year', 'artist_id').parquet(s_song_s3_path)


# ## extract columns to create artists table

# +
# artists - artists in music database
# artist_id, name, location, latitude, longitude
artists_table = spark.sql("""
    SELECT (
        artist_id,
        artist_name AS name,
        artist_location AS location,
        artist_latitude AS latitude,
        artist_longitude AS longitude)
    FROM staging_songs
    ORDER BY artist_id
""")

artists_table.printSchema()
# -

# artists_table = df['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']
artists_table = df.select(
    col("artist_id").alias("artist_id"),
    col("artist_name").alias("name"),
    col("artist_location").alias("location"),
    col("artist_latitude").alias("latitude"),
    col("artist_longitude").alias("longitude"))
artists_table = artists_table.drop_duplicates(subset=['artist_id'])
artists_table.printSchema()

artists_table.limit(1).show()



# ## Load log data

data = './data/logs/*/*/*.json'
df = spark.read.json(data)
df.printSchema()

# filter by actions for song plays
df.createOrReplaceTempView("staging_events")
df = spark.sql("""
    SELECT * FROM staging_events
    WHERE page='NextSong'
    """)

df = df.filter(df.page == 'NextSong')

# ## extract columns for users table

# +
# users - users in the app
# user_id, first_name, last_name, gender, level

users_table = spark.sql("""
    SELECT DISTINCT
        userId AS user_id,
        firstName AS first_name,
        lastName AS last_name,
        gender AS gender,
        level AS level
    FROM staging_events
    ORDER BY user_id
""")

users_table.printSchema()

# -

users_table = df.select(
    col("userId").alias("user_id"),
    col("firstName").alias("first_name"),
    col("lastName").alias("last_name"),
    col("gender").alias("gender"),
    col("level").alias("level"))
users_table = users_table.drop_duplicates(subset=['user_id'])
users_table.printSchema()

# write users table to parquet files
s_s3_path = "{}/users.parquet".format(output_data)
users_table.write.mode('overwrite').parquet(s_s3_path)

# ## extract columns to create time table

# +
gr = spark.sql("""
    SELECT ts
    FROM staging_events
    LIMIT 1
""")

# gr.collect()
gr.show()
# -

df.limit(1).select('ts').show()

# +
# testing convertion
import time

x = 1542241826796

s_format = '%m/%d/%Y %H:%M:%S'
print(time.strftime(s_format, time.localtime(x/1000.)))

# +
# create timestamp column from original timestamp column

import time
from datetime import datetime
from pyspark.sql.functions import udf, col

get_timestamp = udf(lambda x: time.localtime(x/1000.))
get_timestamp = udf(lambda ts: ts/1000)
df2 = df.withColumn('timestamp', get_timestamp('ts'))





# +
# create datetime column from original timestamp column
from pyspark.sql.types import DateType

get_datetime = udf(lambda x: time.strftime(
    '%Y-%m-%d %H:%M:%S', time.localtime(x/1000.0)), DateType())
get_datetime = udf(lambda ts: datetime.fromtimestamp(ts/1000), DateType())
df2 = df2.withColumn('datetime', get_datetime('ts'))


df2.printSchema()

# +
# df2.limit(1).select('datetime').show()

# +
df2.createOrReplaceTempView("staging_events")

gr = spark.sql("""
    SELECT ts
    FROM staging_events
    LIMIT 1
""")

# gr.collect()
# df.select('datetime').show(1)

# +
# df.select('ts').show(1)

# +
# time - timestamps of records in songplays broken down into specific units
# start_time, hour, day, week, month, year, weekday
from pyspark.sql.functions import (year, month, dayofmonth, hour, weekofyear,
                                   date_format, dayofweek)

time_table = spark.sql("""
    SELECT DISTINCT
        datetime AS start_time,
        hour(datetime) AS hour,
        dayofmonth(datetime) AS day,
        weekofyear(datetime) AS week,
        month(datetime) AS month,
        year(datetime) AS year,
        dayofweek(datetime) AS weekday
    FROM staging_events
    ORDER BY start_time
""")

time_table.printSchema()


# +
time_table = df2.select(
    col('datetime').alias('start_time'),
    hour('datetime').alias('hour'),
    dayofmonth('datetime').alias('day'),
    weekofyear('datetime').alias('week'),
    month('datetime').alias('month'),
    year('datetime').alias('year'),
    dayofweek('datetime').alias('weekday')
)
time_table = time_table.drop_duplicates(subset=['start_time'])

time_table.printSchema()

# +
# time_table.select('hour').limit(1).show(1)

# +
# s_s3_path = "{}/time.parquet".format(output_data)
# time_table.write.mode('overwrite').partitionBy(
#     'year', 'month').parquet(s_s3_path)
# -

# ## extract columns from joined song and log datasets to create songplays table

df.select('ts').limit(1).show(1)

df2.printSchema()

# read in song data to use for songplays table
s_s3_path = f"{output_data}/songs.parquet"
# songs_table = spark.read.parquet(s_s3_path)

# +
# NOTE: should've read from parque songs table, but I was uneble to save the data
# locally




# songplays - records in log data associated with song plays i.e. records with page NextSong
# songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

# serial like number:
# source https://towardsdatascience.com/adding-sequential-ids-to-a-spark-dataframe-fa0df5566ff6
# https://stackoverflow.com/questions/53042432/creating-a-row-number-of-each-row-in-pyspark-dataframe-using-row-number-functi

from pyspark.sql.functions import row_number


window = Window.orderBy(col('song_id'))

songplays_table = (df2.join(
    songs_table.alias('songs'), df2.song == col('songs.title'))
     .select(
        col('timestamp').alias('start_time'),
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

songplays_table.printSchema()

# +
df2.createOrReplaceTempView("staging_events")
songs_table.createOrReplaceTempView("songs_table")

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

songplays_table.printSchema()

# +
# songplays_table.limit(1).toPandas()
# -




