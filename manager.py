from pyspark import SparkContext

from batch import BatchClass
from stream import StreamClass

if __name__ == "__main__":
    stream_dir = ""
    master_dir = ""
    db = ""

    sc = SparkContext(appName="Lambda")

    batch = BatchClass(master_dir, stream_dir, db, sc)
    stream = StreamClass(stream_dir, db, sc, 5)
