from time import time
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import os


def total(row):
    lst = []
    tot = 0
    for i in range(1, 145):
        # for j in range(i, i + period):
        # if j < 145:
        tot += int(row.asDict()['TS' + str(i)])
        lst.append(tot)
        # tot = 0
    return lst


if __name__ == "__main__":
    conf = SparkConf()
    conf.setMaster(
        'spark://0.0.0.0:8080').setAppName("Q3")
    sc = SparkContext(conf=conf)
    spark = SparkSession(sc)

    for file_path in ['./data/topic/Economy.csv.csv', './data/topic/Microsoft.csv.csv', './data/topic/Obama.csv.csv', './data/topic/Palestine.csv.csv']:
        topic = file_path.split('/')[-1].replace('.csv', '')
        t_rdd = spark.read.csv(file_path, sep=",", header=True).rdd
        t_rdd_sum = t_rdd.flatMap(lambda x: total(x)).sum()
        t_rdd_avg = t_rdd.flatMap(lambda x: total(x)).mean()
        # print(topic, ' sum :',t_rdd_sum,'\n',topic,' average: ',t_rdd_avg,'\n')

        with open(os.path.join('runs', 'q3_' + topic + '_sum.txt'), '+a') as file:
            file.write(str(t_rdd_sum))
        with open(os.path.join('runs', 'q3_' + topic + '_avg.txt'), '+a') as file:
            file.write(str(t_rdd_avg))
