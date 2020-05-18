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

# ## start Spark

spark

from pyspark.sql import SparkSession
from pyspark.sql.functions import isnan, count, when, col, desc, udf, col, sort_array, asc, avg
from pyspark.sql.functions import sum as Fsum
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType

# +
data_songs = 's3a://udacity-dend/song-data/A/*/*/*.json'
data_logs = 's3a://udacity-dend/log-data/*/*/*.json'

output_data = 's3a://<MY_BUCKET>'
# data = './data/songs/A/*/*.json'
# output_data = './data/output'
# -

df = spark.read.json(data_songs)

df.printSchema()

# ## extract columns to create songs table

songs_table = df['song_id', 'title', 'artist_id', 'year', 'duration']
songs_table.printSchema()

# +
# df.createOrReplaceTempView("staging_songs")
# songs_table = spark.sql("""
#     SELECT song_id, title, artist_id, year, duration
#     FROM staging_songs
#     ORDER BY song_id
# """)  #.collect()

# songs_table.printSchema()
# -

songs_table.limit(1).show()

# write songs table to parquet files partitioned by year and artist
s_song_s3_path = f"s3a://{output_data}/songs.parquet"
songs_table.write.mode('overwrite').partitionBy('year', 'artist_id').parquet(s_song_s3_path)

# ## extract columns to create artists table

# artists_table = df['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']
artists_table = df.select(
    col("artist_id").alias("artist_id"),
    col("artist_name").alias("name"),
    col("artist_location").alias("location"),
    col("artist_latitude").alias("latitude"),
    col("artist_longitude").alias("longitude"))
artists_table = artists_table.drop_duplicates(subset=['artist_id'])
artists_table.printSchema()

# write songs table to parquet files partitioned by year and artist
s_s3_path = "s3a://{}/artists.parquet".format(output_data)
artists_table.write.mode('overwrite').parquet(s_s3_path)

# ## filter by actions for song plays

df = spark.read.json(data_logs)
df = df.filter(df.page == 'NextSong')
df.printSchema()

# ## extract columns for users table

users_table = df.select(
    col("userId").alias("user_id"),
    col("firstName").alias("first_name"),
    col("lastName").alias("last_name"),
    col("gender").alias("gender"),
    col("level").alias("level"))
users_table = users_table.drop_duplicates(subset=['user_id'])
users_table.printSchema()

users_table.limit(1).show()

# write users table to parquet files
s_s3_path = "{}/users.parquet".format(output_data)
users_table.write.mode('overwrite').parquet(s_s3_path)


# ## extract columns to create time table

# +
from pyspark.sql.types import DateType
from datetime import datetime
import time

# create datetime column from timestamp column
get_timestamp = udf(lambda x: time.localtime(x/1000.))
get_datetime = udf(lambda x: time.strftime(
    '%Y-%m-%d %H:%M:%S', time.localtime(x/1000.0)), DateType())

get_timestamp = udf(lambda ts: ts/1000)
get_datetime = udf(lambda ts: datetime.fromtimestamp(ts/1000), DateType())

df = df.withColumn('timestamp', get_timestamp('ts'))
df = df.withColumn('datetime', get_datetime('ts'))
df.printSchema()
# -

df.select('timestamp', 'datetime').limit(1).show(1)



# +

from pyspark.sql.functions import (year, month, dayofmonth, hour, weekofyear,
                                   date_format, dayofweek)

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

time_table.printSchema()
# -

time_table.limit(1).show()

s_s3_path

# write time table to parquet files partitioned by year and month
s_s3_path = "{}/time.parquet".format(output_data)
time_table.write.mode('overwrite').partitionBy(
    'year', 'month').parquet(s_s3_path)

# ## extract columns from joined song and log datasets to create songplays table

s_s3_path

# read in song data to use for songplays table
s_s3_path = "s3a://{}/songs.parquet".format(output_data)
songs_table = spark.read.parquet(s_s3_path)

songs_table.limit(1).show()

# +
from pyspark.sql.functions import row_number


window = Window.orderBy(col('song_id'))

songplays_table = (df.join(
    songs_table.alias('songs'), df.song == col('songs.title'))
     .select(
        col('timestamp').alias('start_time'),
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

songplays_table.printSchema()
# -

songplays_table.limit(1).show()

# write songplays table to parquet files partitioned by year and month
s_s3_path = "{}/songplays.parquet".format(output_data)
songplays_table.write.partitionBy(
    ['year', 'month']).mode('overwrite').parquet(s_s3_path)


