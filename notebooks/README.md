You can download data from S3 after installing AWS CLI in your local machine.
In this folder, use the following commands:

```shell
$ mkdir data/
$ mkdir data/songs/
$ mkdir data/logs/
$ aws s3 cp <SONG_DATA_S3_PATH> data/songs --recursive
$ aws s3 cp <LOG_DATA_S3_PATH> data/songs --recursive
```
