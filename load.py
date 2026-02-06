import sys

from requests import get
from enum import Enum
from argparse import ArgumentParser, Namespace
from logging import Logger, getLogger
from typing import Optional

from pyspark.sql import DataFrame, SparkSession

L: Logger = getLogger(__name__)

class Arguments(Enum):
    DESCRIPTION = "Data ingestion and Delta table creation workflow."
    API = 'Override the SODA API URL for data download.'
    DELTA_PATH = 'Override the Delta table storage path.'

class DatasetConfig(Enum):
    API_URL = "https://data.cityofchicago.org/resource/85ca-t3if.json"
    LIMIT_PARAM = "$limit"
    DEFAULT_LIMIT = 100000

class DeltaConfig(Enum):
    TABLE_PATH = "/tmp/traffic_crashes_delta"
    FORMAT = "delta"
    MODE = "overwrite"        

class SparkConfig(Enum):
    DELTA_PACKAGE = ""
    DELTA_EXTENSION = ""
    DELTA_CATALOG = ""

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


def log_override(args: Namespace) -> None:    
    if args.api:
        L.warning("OVERRIDE `api: str = '{}'`".format(args.api))
    if args.delta_path:
        L.warning("OVERRIDE `delta_path: str = '{}'`".format(args.delta_path))

def get_args(*args: str):
    parser = ArgumentParser(description=Arguments.DESCRIPTION.value)
    parser.add_argument('--api', type=str, help=Arguments.API.value)
    parser.add_argument('--delta_path', type=str, help=Arguments.DELTA_PATH.value)

    return parser.parse_args(args)
    
if __name__ == "__main__":
    args: Namespace = get_args(*sys.argv[1:])
    log_override(args)

    main(SparkSession
        .builder
        .config("spark.jars.packages", SparkConfig.DELTA_PACKAGE.value) 
        .config("spark.sql.extensions", SparkConfig.DELTA_EXTENSION.value) 
        .config("spark.sql.catalog.spark_catalog", SparkConfig.DELTA_CATALOG.value)
        .getOrCreate(), 
        **args.__dict__)
