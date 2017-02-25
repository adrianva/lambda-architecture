import sys
import shutil
import errno
from pyspark.sql import Row, SparkSession


def copy(src, dest):
    try:
        shutil.copytree(src, dest)
    except OSError as e:
        # If the error was caused because the source wasn't a directory
        if e.errno == errno.ENOTDIR:
            shutil.copy(src, dest)
        else:
            print('Directory not copied. Error: %s' % e)


def save_to_mongo(rdd, collection):
    try:
        # Get the singleton instance of SparkSession
        spark = get_spark_session_instance(rdd.context.getConf())

        # Convert RDD[String] to RDD[Row] to DataFrame
        row_rdd = rdd.map(lambda word_count: Row(word=word_count[0], count=word_count[1]))
        words_data_frame = spark.createDataFrame(row_rdd)

        words_data_frame.write.format("com.mongodb.spark.sql")\
            .mode("overwrite")\
            .option("spark.mongodb.output.uri", "mongodb://localhost:27017/kschool." + collection)\
            .save()
    except BaseException:
        print sys.exc_info()


def get_spark_session_instance(spark_conf):
    if 'sparkSessionSingletonInstance' not in globals():
        globals()['sparkSessionSingletonInstance'] = SparkSession\
            .builder\
            .config(conf=spark_conf)\
            .getOrCreate()
    return globals()['sparkSessionSingletonInstance']