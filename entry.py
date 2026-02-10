import sys

from argparse import ArgumentParser, Namespace
from importlib import import_module
from typing import Callable

from pyspark.sql import SparkSession

if __name__ == "__main__":
    args: Namespace
    spark = SparkSession.builder.getOrCreate()
    parser = ArgumentParser(description="Ingest data into Spark and save as Parquet.")    
    
    parser.add_argument('--job', type=str, help='Python package name.')
    parser.add_argument('--task', type=str, help='Python module name.')
    parser.add_argument('--api_url', type=str, help='API URL to fetch data from.')
    parser.add_argument('--target_path', type=str, help='Target path to save Parquet data.')
    
    args, unknown = parser.parse_known_args()
    
    ROOT: str = 'crashes'
    module: str = "{package}.{module}".format(package=args.job, module=args.task)
    
    if ROOT not in sys.path:
        sys.path.insert(0, ROOT)
        print(f"Added '{ROOT}' to sys.path")
    else:
        print(f"'{ROOT}' already in sys.path")

    main: Callable[[SparkSession, Namespace], None] = import_module(module).main
    main(spark, args)
