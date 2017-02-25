import threading
import glob
from pyspark.sql import SparkSession

import utils


class BatchClass(threading.Thread):
    def run(self):
        print "Starting: " + self.name
        # TODO Mover ficheros


def get_word_count(texts):
    return texts.flatMap(lambda line: line.split(" "))\
        .filter(lambda line: line != "")\
        .map(lambda word: (word, 1))\
        .reduceByKey(lambda a, b: a + b)


if __name__ == "__main__":
    spark = SparkSession.builder.appName("Batch Layer").getOrCreate()

    sc = spark.sparkContext
    matches_without_goals = sc.accumulator(0)

    utils.copy("HDFS/new", "HDFS/master")

    # directories = glob.glob("HDFS/master/*")
    # texts_files = []
    # for directory in directories:
    #     texts = sc.wholeTextFiles(directory)
    #     word_count = get_word_count(texts)
    #     utils.save_to_mongo(word_count, collection="batch_view")

    texts = sc.textFile("HDFS/master/*/*")
    word_count = get_word_count(texts)
    utils.save_to_mongo(word_count, collection="batch_view")
