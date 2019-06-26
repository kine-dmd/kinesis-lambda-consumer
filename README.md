# Kinesis lambda consumer for Apple Watch 3 (deprecated) [![Build Status](https://travis-ci.org/kine-dmd/kinesis-lambda-consumer.svg?branch=master)](https://travis-ci.org/kine-dmd/kinesis-lambda-consumer)

The Kinesis lambda consumer is designed to read raw binary data files from a Kinesis shard, parse them into numeric data, combine the data that comes from the same watch, convert the data to parquet, and write the data to an S3 file. This is illustrated below:
![kinesisLambda](https://user-images.githubusercontent.com/26333869/60194531-695c8000-9831-11e9-8748-c92150e59b89.png)
