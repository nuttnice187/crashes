import sys

from requests import get
from enum import Enum
from argparse import ArgumentParser, Namespace
from logging import Logger, getLogger
from typing import Optional

from pyspark.sql import DataFrame, SparkSession

L: Logger = getLogger(__name__)

class DatasetConfig(Enum):
    API_URL = "https://data.cityofchicago.org/resource/85ca-t3if.json"
    LIMIT_PARAM = "$limit"
    DEFAULT_LIMIT = 100000

class DeltaConfig(Enum):
    TABLE_PATH = "/tmp/traffic_crashes_delta"
    FORMAT = "delta"
    MODE = "overwrite"        

class Crashes:

    def __init__(self, data: DataFrame) -> None:
        pass

    def write(self) -> None:
        pass


def main(
    spark: SparkSession,
    api: Optional[str]=None, 
    delta_path: Optional[str]=None
    ) -> None:
    if not api:
        api = DatasetConfig.API_URL.value
    if not delta_path:
        delta_path = DeltaConfig.TABLE_PATH.value
    
    crashes = Crashes(get_data(spark, api))
    crashes.write(delta_path)
      
    spark.stop()

if __name__ == "__main__":
    parser = ArgumentParser(description="Data ingestion and Delta table creation workflow.")
    parser.add_argument('--api', type=str, help='Override the SODA API URL for data download.')
    parser.add_argument('--delta_path', type=str, help='Override the Delta table storage path.')

    args: Namespace = parser.parse_args(sys.argv[1:])
    
    if args.api:
        L.warning("OVERRIDE api: str = '{}'".format(args.api))
    if args.delta_path:
        L.warning("OVERRIDE delta_path: str = '{}'".format(args.delta_path))

    main(SparkSession
        .builder
        .config("spark.jars.packages", SparkConfig.DELTA_PACKAGE.value) 
        .config("spark.sql.extensions", SparkConfig.DELTA_EXTENSION.value) 
        .config("spark.sql.catalog.spark_catalog", SparkConfig.DELTA_CATALOG.value)
        .getOrCreate(), 
        **args.__dict__)
