from time import time
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import os


def count(row):
    lst = []
    for c in row:
        lst.append((c, 1))
    return lst


def parse_by_hour(row, period):
    lst = []
    tot = 0
    for i in range(2, 145, period):
        for j in range(i, i + period):
            if j < 145:
                tot += int(row.asDict()['TS' + str(i)])
        lst.append(tot)
        tot = 0
    return lst


period_dct = {3: 'hour', 72: 'day'}
if __name__ == "__main__":
    conf = SparkConf()
    conf.setMaster(
        'spark://0.0.0.0:8080').setAppName("Q2")
    sc = SparkContext(conf=conf)
    spark = SparkSession(sc)

    # hour
    for file_path in ['./data/platform/Facebook.csv', './data/platform/GooglePlus.csv', './data/platform/LinkedIn.csv']:
        platform = file_path.split('/')[-1].replace('.csv', '')
        df = spark.read.csv(file_path, sep=',', header=True).rdd

        for period in [3, 72]:
            rdd_avg = df.flatMap(lambda x: parse_by_hour(x, period))
            rdd_avg = rdd_avg.mean()
            with open(os.path.join('runs', 'q2_' + platform + '_' + period_dct[period] + '_avg.txt'), '+a') as file:
                file.write(str(rdd_avg))
