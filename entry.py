import sys

from data_capture.ingestion import main
from pyspark.sql import SparkSession

if __name__ == "__main__":
    print(sys.argv)
    project_root = 'crashes'
    
    if project_root not in sys.path:
        sys.path.insert(0, project_root)
        print(f"Added '{project_root}' to sys.path")
    else:
        print(f"'{project_root}' already in sys.path")

    # spark = SparkSession.builder.getOrCreate()
    # main(spark)
