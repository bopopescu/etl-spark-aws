Data Warehouse using AWS
===========================

This project is part of the [Data Engineering Nanodegree](https://www.udacity.com/course/data-engineer-nanodegree--nd027) program, from Udacity. I manipulate data for a music streaming app called Sparkify, where I write an ETL pipeline for a data lake hosted on S3.

Currently, the startup has grown its user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app. This data initially  is pulled from S3 and processed using Spark to split it into 5 new tables, each one containing parts of the data from the logs files:

- users: Dimension table. Users in the app.
- songs: Dimension table. Songs in music database.
- artists: Dimension table. Artists in music database.
- time: Dimension table. Timestamps of records in songplays broken down into units.
- songplays: Fact table. Records in log data associated with song plays.

This set of dimensional and fact tables are loaded back into S3 as parquet files. The schema proposed would help them analyze the data they’ve been collecting on songs and user activity on their app using Spark or even simple SQL queries on the tables through Spark.


### Install
To set up your python environment to run the code in this repository, start by
 creating a new environment with Anaconda and install the dependencies.

```shell
$ conda create --name ngym36 python=3.6
$ source activate ngym36
$ pip install -r requirements.txt
```

### Run
In a terminal or command window, navigate to the top-level project directory (that contains this README). You need to set up a [EMR](https://aws.amazon.com/pt/emr/) cluster. So, start by renaming the file `confs/dl.template.cfg` to  `confs/dl.cfg` and fill in the `KEY` and `SECRET` in the AWS section and `DL_CODE_BUCKET_NAME` and `DL_DATA_BUCKET_NAME` with the name of a S3 bucket. Then, enter the following command:

```shell
$ python iac.py -i
$ python iac.py -u
$ python iac.py -r
$ watch -n 15 'python iac.py -s'
```

The above instructions are going to create the IAM role, upload a copy of the `etl.py` file to your S3, create an EMR cluster, and check the status of this cluster every 15 seconds. Fill in the other fields from your `dl.cfg` that shows up in the commands console outputs. After Amazon finally launch your cluster, run:

```shell
$ python iac.py -r
```

This command is going to run the ETL pipeline in the EMR cluster to create all the fact and dimensional tables mentioned above. Finally, CLEAN UP your resources using the commands below:

```shell
$ python iac.py -da
$ watch -n 15 'python iac.py -s'
```

Wait for the second command shows the status `TERMINATED`.

### License
The contents of this repository are covered under the [MIT License](LICENSE).
