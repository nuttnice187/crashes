import sys

from argparse import ArgumentParser, Namespace
from importlib import import_module

from logging import Logger, getLogger, INFO
from typing import Callable

from pyspark.sql import SparkSession

L: Logger = getLogger(__name__)
L.setLevel(INFO)

if __name__ == "__main__":    
    args: Namespace
    parser = ArgumentParser(description="Ingest data from API into Spark and save as Parquet."
        "Curate silver delta table from ingested datasource."
        "Present gold delta table from curated change feed."
        )    
    
    parser.add_argument('--job', type=str, help='Python package name.')
    parser.add_argument('--task', type=str, help='Python module name.')
    parser.add_argument('--api_url', type=str, help='API URL to fetch data from.')
    parser.add_argument('--source_path', type=str, help='Source path to read from.')
    parser.add_argument('--target_path', type=str, help='Target path to write to.')
    parser.add_argument('--run_id', type=str, help='databricks metadata for job run.')
    
    args, unknown = parser.parse_known_args()
    L.info(args)
    
    ROOT: str = 'crashes'
    module: str = "{package}.{module}".format(package=args.job, module=args.task)
    
    if ROOT not in sys.path:
        sys.path.insert(0, ROOT)
        L.info(f"Added '{ROOT}' to sys.path")
    else:
        L.info(f"'{ROOT}' already in sys.path")

    main: Callable[[SparkSession, Logger, Namespace], None] = import_module(module).main
    main(SparkSession.builder.getOrCreate(), L, args)
