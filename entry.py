import sys

from data_capture.ingestion import main
from pyspark.sql import SparkSession

if __name__ == "__main__":
    print(sys.argv)
    main(SparkSession.builder.getOrCreate())
