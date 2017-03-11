# -*- coding: utf-8 -*-
import sys
import datetime
import threading

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import SQLContext, Row
from pyspark.ml.feature import RegexTokenizer

from pymongo import MongoClient


class StreamClass(threading.Thread):
    # Kafka connection
    DEFAULT_BROKER = 'localhost:9092'
    DEFAULT_TOPIC = ['test']

    def __init__(self, spark_context=None, batch_duration=5, brokers=DEFAULT_BROKER, topics=DEFAULT_TOPIC):
        super(StreamClass, self).__init__()
        self.spark_context = spark_context
        self.streaming_context = StreamingContext(spark_context, batchDuration=batch_duration)
        self.sql_context = SQLContext(spark_context)
        self.streaming_context.checkpoint("checkpoint")

        self.kvs = KafkaUtils.createDirectStream(self.streaming_context, topics, {"metadata.broker.list": brokers})

    def run(self):
        print "Starting Stream Layer: " + self.name
        # Kafka emits tuples, so we need to acces to the second element
        lines = self.kvs.map(lambda line: line[1]).cache()

        # save to HDFS
        lines.foreachRDD(save_stream)

        words = get_words(lines)
        word_count = words.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)
        word_count.foreachRDD(lambda rdd: rdd.foreachPartition(save_to_mongo))

        self.streaming_context.start()
        self.streaming_context.awaitTermination()


def save_stream(rdd):
    rdd.saveAsTextFile("HDFS/new/" + datetime.datetime.now().strftime("%H%M%S"))


def get_words(lines):
    words = lines.flatMap(lambda line: line.split())
    return words


def word_tokenize(line):
    import nltk
    return nltk.word_tokenize(line)


def save_words(time, rdd):
    print("========= %s =========" % str(time))
    try:
        rdd.map(lambda word: save_to_mongo(word[0]))
    except BaseException:
        print sys.exc_info()


def save_to_mongo(iter):
    client = MongoClient('mongodb://localhost:27017/')
    db = client.kschool

    for record in iter:
        saved_word = db.rt_view1.replace_one(
            {"word": record[0]},
            {"word": record[0], "count": record[1]},
            upsert=True
        )


if __name__ == "__main__":
    sc = SparkContext(appName="Stream Layer", master="local[2]")
    ssc = StreamingContext(sc, 10)
    sql_context = SQLContext(sc)
    ssc.checkpoint("checkpoint")

    # Kafka connection
    brokers = 'localhost:9092'
    topics = ["test"]

    kvs = KafkaUtils.createDirectStream(ssc, topics, {"metadata.broker.list": brokers})
    # Kafka emits tuples, so we need to acces to the second element
    lines = kvs.map(lambda line: line[1]).cache()

    # save to HDFS
    lines.foreachRDD(save_stream)

    words = get_words(lines)
    word_count = words.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)
    word_count.foreachRDD(lambda rdd: rdd.foreachPartition(save_to_mongo))

    ssc.start()
    ssc.awaitTermination()
