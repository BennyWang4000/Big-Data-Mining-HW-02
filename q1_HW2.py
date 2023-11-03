from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from nltk.tokenize import word_tokenize
import pandas as pd
import pyspark.pandas as ps
import os


def parse_words(row, col):
    tokens = word_tokenize(str(row.asDict()[col]))
    res = []
    for token in tokens:
        res.append((token, 1))
    return res


def parse_words_per(row, per, col):
    tokens = word_tokenize(str(row.asDict()[col]))
    res = []
    for token in tokens:
        res.append(((row.asDict()[per], token), 1))
    return res


if __name__ == '__main__':

    DATA_PATH = './data/News_Final_clean.csv'

    conf = SparkConf()
    conf.setMaster(
        'spark://DESKTOP-I95HRMP.localdomain:12222').setAppName("Q1")
    sc = SparkContext(conf=conf)
    spark = SparkSession(sc)

    rows = spark.read.csv(DATA_PATH, sep=',', header=True).rdd

    for col in ['Title', 'Headline']:
        # * ====================== in total ======================
        rows.flatMap(lambda x: parse_words(x, col))\
            .reduceByKey(lambda x1, x2: x1 + x2)\
            .sortBy(lambda x: x[1], ascending=False).saveAsTextFile(
                os.path.join('runs', 'q1_in_total_' + col))

        # * ====================== per day or per topic ======================
        for per in ['PublishDate', 'Topic']:
            rows.flatMap(lambda x: parse_words_per(x, per, col))\
                .reduceByKey(lambda x1, x2: x1 + x2)\
                .sortBy(lambda x: x[1], ascending=False).saveAsTextFile(
                    os.path.join('runs', 'q1_' + per + '_' + col))
