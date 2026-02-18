import json

from argparse import Namespace
from enum import Enum
from logging import Logger
from typing import Dict, List, Optional

from urllib3.util.retry import Retry

from requests import get, Response, Session
from requests.adapters import HTTPAdapter

from pyspark.sql import DataFrame, DataFrameWriter, SparkSession
from pyspark.sql.functions import col, current_timestamp, to_timestamp, year
from pyspark.sql.types import StringType


def dump_location(source: List[Dict]) -> List[Dict]:
    """
    convert location values of the api response to a json string
    """
    result = source.copy()
    for item in result:
        if 'location' in item and item['location'] is not None:
            location_value = item['location']
            if isinstance(location_value, (dict, list)):
                item['location'] = json.dumps(location_value)
            else:
                item['location'] = str(location_value)
    return result


class Default(Enum):
    """
    Enumerate default parameter values
    """
    API_URL = "https://data.cityofchicago.org/resource/85ca-t3if.json"
    TARGET_PATH = "/content/drive/MyDrive/crashes_data"

class Target(Enum):
    """
    Enamerate target delta table properties
    """
    RENAMED = (":@computed_region_rpca_8um6", "computed_region_rpca_8um6")
    PRIMARY_KEY = "crash_record_id"
    PARTITION = ("crash_month", "crash_year")
    
class Ingestor:
    """
    Ingests data from a given API URL, transforms it, and loads it into a Parquet file.
    """
    logger: Logger
    source: List[Dict]
    target: DataFrame

    def __init__(
            self, spark: SparkSession, logger: Logger, api_url: str, target_path: str
        ) -> None:
        """
        Initializes the Ingestor with the provided SparkSession, API URL, and target path.
        """
        self.logger = logger
        self.extract(api_url)
        self.transform(spark, target_path)
        self.load(target_path)

    def extract(self, api_url: str) -> None:
        """
        Fetches data from a given API URL and creates a Spark DataFrame from it.
        Includes retry mechanism for network resilience.
        """
        retries = Retry(total=5, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
        session = Session()
        session.mount("https://", HTTPAdapter(max_retries=retries))

        response: Optional[Response] = None
        try:
            response = session.get(api_url)
            response.raise_for_status()
        except Exception as e:
            self.logger.error(f"Failed to fetch data from {api_url} after retries: {e}")
            raise

        self.source = response.json()

    def transform(self, spark: SparkSession, target_path: str) -> None:
        """
        Preprocesses 'location' field and handles existing data for incremental loading.
        """
        self.target = (spark.createDataFrame(dump_location(self.source))
            .select('*',
                    year(to_timestamp(col("crash_date"), "yyyy-MM-dd'T'HH:mm:ss.SSS"))
                    .cast(StringType()).alias("crash_year"),
                    current_timestamp().alias("ingest_date")
                   )
            .withColumnRenamed(*Target.RENAMED.value))

        self.check_existing(spark, target_path)

    def check_existing(self, spark: SparkSession, target_path: str) -> None:
        """
        Checks for existing data in the target path and filters transformation for incremental loading.
        """
        existing: Optional[DataFrame] = None
        
        try:
            existing = spark.read.parquet(target_path)
            self.logger.info(f"Successfully read existing Parquet data from {target_path}. Row count: {existing.count()}")
        except Exception as e:
            self.logger.warning(f"Could not read existing Parquet data from {target_path}: {e}. Proceeding without existing data.")

        if existing is not None and not existing.isEmpty():
            primary_key = Target.PRIMARY_KEY.value
            if primary_key in self.target.columns and primary_key in existing.columns:
                self.target = self.target.join(existing, on=primary_key, how='left_anti')
                self.logger.info(f"Performed left_anti join with existing data on '{primary_key}'. New records count: {self.target.count()}")
            else:
                self.logger.warning(f"Cannot perform left_anti join: '{primary_key}' not found in one or both dataframes. Loading all data from source.")
        else:
            self.logger.info("No existing data to join with, or existing data is empty. Loading all data from source.")

    def load(self, target_path: str) -> None:
        """
        Writes the transformed data to a Parquet file in the specified target path.
        """
        writer: DataFrameWriter = (self.target.write
                                   .mode('append').partitionBy(*Target.PARTITION.value))
        writer.parquet(target_path)


def main(
        spark: SparkSession, logger: Logger, args: Namespace
    ) -> None:
    """
    Instantiates the Ingestor class with the provided arguments.
    """
    Ingestor(
        spark=spark,
        logger=logger,
        api_url=args.api_url if args.api_url else Default.API_URL.value,
        target_path=args.target_path if args.target_path else Default.TARGET_PATH.value
    )
