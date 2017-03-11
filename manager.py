from pyspark import SparkContext

from batch import BatchClass
from stream import StreamClass

if __name__ == "__main__":
    stream_dir = "HDFS/new"
    master_dir = "HDFS/master"
    db = "kschool"

    sc = SparkContext(appName="Lambda")

    batch = BatchClass(sc)
    stream = StreamClass(sc, 5)
    batch.start()
    stream.start()

    while True:
        if not batch.is_alive:
            print "Starting batch process..."
            batch.start()
