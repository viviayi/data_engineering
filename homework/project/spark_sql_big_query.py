#!/usr/bin/env python
# coding: utf-8

import argparse

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


parser = argparse.ArgumentParser()

parser.add_argument('--input_ASBO', required=True)
parser.add_argument('--output', required=True)

args = parser.parse_args()

input_ASBO = args.input_ASBO
output = args.output

spark = SparkSession.builder \
    .appName('test') \
    .getOrCreate()

spark.conf.set('temporaryGcsBucket', 'dataproc-staging-europe-west2-456166370442-0umjd87f')

df_data = spark.read.parquet(input_ASBO)

df_data.registerTempTable('ASBO_data')

df_data.write.format('bigquery') \
    .option('table', output) \
    .save()
    



