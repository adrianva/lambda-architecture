import threading
from pyspark.sql import SparkSession

import utils


class BatchClass(threading.Thread):
    def __init__(self, spark_context=None):
        super(BatchClass, self).__init__()
        self.spark_context = spark_context

    def run(self):
        print "Starting Batch Layer: " + self.name
        batch_processing(self.spark_context)


def get_word_count(texts):
    return texts.flatMap(lambda line: line.split(" "))\
        .filter(lambda line: line != "")\
        .map(lambda word: (word, 1))\
        .reduceByKey(lambda a, b: a + b)


def batch_processing(sc):
    try:
        utils.copy("HDFS/new/", "HDFS/master/")

        texts = sc.textFile("HDFS/master/*/*")
        if texts:
            word_count = get_word_count(texts)
            utils.save_to_mongo(word_count, database="kschool", collection="batch_view")
    except OSError:
        print "New directory is empty..."


if __name__ == "__main__":
    spark = SparkSession.builder.appName("Batch Layer").getOrCreate()

    sc = spark.sparkContext
    batch_processing(sc)
