#!/bin/bash

/opt/spark/bin/spark-submit --master yarn-client --executor-memory 4G --executor-cores 3 --principal demtv/adopt@HADOOP.SECURE --keytab ~/demtv.keytab target/tke-0.0.1-SNAPSHOT-run.jar /data/Parquet/AdnLog/2018_07_04 /data/Parquet/AdnLog/2018_07_05 /data/Parquet/AdnLog/2018_07_06 /data/Parquet/AdnLog/2018_07_07 /data/Parquet/AdnLog/2018_07_08 /data/Parquet/AdnLog/2018_07_09 /data/Parquet/AdnLog/2018_07_10 /data/Parquet/AdnLog/2018_07_11 /data/Parquet/AdnLog/2018_07_12 /data/Parquet/AdnLog/2018_07_13 /data/Parquet/AdnLog/2018_07_14 /data/Parquet/AdnLog/2018_07_15 /data/Parquet/AdnLog/2018_07_16 /data/Parquet/AdnLog/2018_07_17 /data/Parquet/AdnLog/2018_07_18 /user/demtv/log_cate50_new

