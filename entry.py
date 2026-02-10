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

    parser = ArgumentParser(description="Ingest data into Spark and save as Parquet.")
    parser.add_argument('--api_url', type=str, help='API URL to fetch data from.')
    parser.add_argument('--target_path', type=str, help='Target path to save Parquet data.')

    args, unknown = parser.parse_known_args()

    # spark = SparkSession.builder.getOrCreate()
    # main(spark)
