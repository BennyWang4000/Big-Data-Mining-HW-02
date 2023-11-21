from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from nltk.tokenize import word_tokenize
import numpy as np
import pandas as pd
import os


def parse_words(row, col):
    tokens = word_tokenize(str(row.asDict()[col]))
    res = []
    for token in tokens:
        res.append((token, 1))
    return res


def parse_cooccurrence(row, col, words):
    res = []
    for i, (word_a, _) in enumerate(words):
        for word_b, _ in words[i:]:
            try:
                if word_a in row.asDict()[col] and word_b in row.asDict()[col]:
                    res.append(((word_a, word_b), 1))
            except Exception as e:
                print('NoneType')
    return res


if __name__ == '__main__':

    DATA_PATH = './data/News_Final_clean.csv'
    OUTPUT_DIR = './runs'

    conf = SparkConf()
    conf.setMaster(
        'spark://0.0.0.0:8080').setAppName("Q4")
    sc = SparkContext(conf=conf)
    spark = SparkSession(sc)

    rows = spark.read.csv(DATA_PATH, sep=',', header=True).rdd

    for col in ['Title', 'Headline']:
        words = rows.flatMap(lambda row: parse_words(row, col))\
                    .reduceByKey(lambda times1, times2: times1 + times2)\
                    .takeOrdered(100, key=lambda times: -times[1])
        for topic in ['economy', 'microsoft', 'obama', 'palestine']:
            coocu_mat = [[]] * 101
            coocu = rows.filter(lambda row: row.asDict()['Topic'] == topic)\
                        .flatMap(lambda row: parse_cooccurrence(row, col, words))\
                        .reduceByKey(lambda times1, times2: times1 + times2)\
                        .sortBy(lambda pair: pair[0][0])\
                        .collect()
            tags = []
            for (t1, t2), _ in coocu:
                tags += [t1, t2]
            tags = index = columns = sorted(list(set(tags)))
            tags = dict((t, i) for i, t in enumerate(tags))
            coocu_mat = np.identity(len(tags))
            for (t1, t2), co in coocu:
                coocu_mat[tags[t1]][tags[t2]] = co
                coocu_mat[tags[t2]][tags[t1]] = co
            df = pd.DataFrame(coocu_mat, index=index, columns=columns)
            df.to_csv(OUTPUT_DIR + '/q4_' + col + '_' + topic + '.csv')
