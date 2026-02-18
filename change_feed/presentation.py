from argparse import Namespace
from logging import Logger
from pyspark.sql import SparkSession


def main(spark: SparkSession, logger: Logger, args: Namespace) -> None:
    """
    Read from curated delta table and apply any required
    1. Filter to specific level of detail
    2. Aggregation, summarization
    3. Write to bronze delta table
    """
    pass
