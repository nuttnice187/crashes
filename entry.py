import sys
from importlib import import_module
from argparse import ArgumentParser, Namespace
from data_capture.ingestion import main
from pyspark.sql import SparkSession
from typing import Callable

if __name__ == "__main__":
    print(sys.argv)
    ROOT: str = 'crashes'
    parser = ArgumentParser(description="Ingest data into Spark and save as Parquet.")    
    
    parser.add_argument('--job', type=str, help='Python package name.')
    parser.add_argument('--task', type=str, help='Python module name.')
    parser.add_argument('--api_url', type=str, help='API URL to fetch data from.')
    parser.add_argument('--target_path', type=str, help='Target path to save Parquet data.')

    args: Namespace
    args, unknown = parser.parse_known_args()
    
    if ROOT not in sys.path:
        sys.path.insert(0, ROOT)
        print(f"Added '{ROOT}' to sys.path")
    else:
        print(f"'{ROOT}' already in sys.path")
        
    print('.'.join((args.job, args.task)))
    
    main: Callable[[SparkSession, Namespace], None] = import_module((args.job, args.task).join('.')).main

    

    # spark = SparkSession.builder.getOrCreate()
    # main(spark, args)
