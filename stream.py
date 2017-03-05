# -*- coding: utf-8 -*-
import sys
import datetime
import threading

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import SQLContext, Row, SparkSession
from pyspark.ml.feature import RegexTokenizer

import utils


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

        words = lines.flatMap(lambda line: line.split(" "))
        words.foreachRDD(compute_word_count)

        self.streaming_context.start()
        self.streaming_context.awaitTermination()


def save_stream(rdd):
    rdd.saveAsTextFile("HDFS/new/" + datetime.datetime.now().strftime("%H%M%S"))


def compute_word_count(time, rdd):
    print("========= %s =========" % str(time))
    try:
        # Get the singleton instance of SparkSession
        spark = utils.get_spark_session_instance(rdd.context.getConf())
        tokenizer = RegexTokenizer(inputCol="text", outputCol="words", pattern="\\W")

        # Convert RDD[String] to RDD[Row] to DataFrame
        row_rdd = rdd.map(lambda line: Row(text=line))
        df = spark.createDataFrame(row_rdd)

        tokens_rdd = tokenizer.transform(df).head()
        words_data_frame = spark.createDataFrame([tokens_rdd])

        # Creates a temporary view using the DataFrame.
        words_data_frame.createOrReplaceTempView("text")

        # Do word count on table using SQL and print it
        word_count_data_frame = spark.sql(
            "select word, count(*) as count "
            "from ("
            "select explode(words) as word from text"
            ") words "
            "group by word"
        )

        word_count_data_frame.write.format("com.mongodb.spark.sql") \
            .mode("overwrite")\
            .option("spark.mongodb.output.uri", "mongodb://localhost:27017/kschool.rt_view1") \
            .save()

        word_count_data_frame.write.format("com.mongodb.spark.sql") \
            .mode("overwrite") \
            .option("spark.mongodb.output.uri", "mongodb://localhost:27017/kschool.rt_view2") \
            .save()
    except BaseException:
        print sys.exc_info()


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
    lines = kvs.map(lambda line: line[1])

    # save to HDFS
    lines.foreachRDD(save_stream)
    lines.foreachRDD(compute_word_count)

    #words = lines.flatMap(lambda line: line.split(" "))
    #words.foreachRDD(compute_word_count)

    ssc.start()
    ssc.awaitTermination()
