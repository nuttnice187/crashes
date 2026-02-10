import sys
import json
import time

from argparse import ArgumentParser, Namespace
from enum import Enum
from logging import Logger, getLogger, INFO
from requests import get, Response, Session
from typing import Dict, List, Optional

from requests.adapters import HTTPAdapter
from pyspark.sql import DataFrame, SparkSession

from pyspark.sql.functions import current_timestamp
from urllib3.util.retry import Retry

L: Logger = getLogger(__name__)
L.setLevel(INFO)

class Default(Enum):
    API_URL = "https://data.cityofchicago.org/resource/85ca-t3if.json"
    TARGET_PATH = "/content/drive/MyDrive/crashes_data" # Changed target path to a specific subdirectory

class Ingestor:
    """
    Ingests data from a given API URL, transforms it, and loads it into a Parquet file.
    """
    api_url: str
    target_path: str
    source: List[Dict]
    target: DataFrame

    def __init__(self, spark: SparkSession, api_url: str, target_path: str) -> None:
        """
        Initializes the Ingestor with the provided SparkSession, API URL, and target path.
        """
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
            response.raise_for_status()  # Raise an exception for bad status codes
        except Exception as e:
            L.error(f"Failed to fetch data from {api_url} after retries: {e}")
            raise # Re-raise the exception if all retries fail

        self.source: List[Dict] = response.json()

    def transform(self, spark: SparkSession, target_path: str) -> None:
        """
        Preprocesses 'location' field and handles existing data for incremental loading.
        """
        source_copy = self.source.copy()
        for item in source_copy:
            if 'location' in item and item['location'] is not None:
                location_value = item['location']
                if isinstance(location_value, (dict, list)):
                    item['location'] = json.dumps(location_value)
                else:
                    item['location'] = str(location_value)

        self.target: DataFrame = (
            spark.createDataFrame(source_copy)
                .withColumn('update_time', current_timestamp())
                .withColumnRenamed(":@computed_region_rpca_8um6", "computed_region_rpca_8um6")
        )

        self.check_existing(spark, target_path)

    def check_existing(self, spark: SparkSession, target_path: str) -> None:
        """
        Checks for existing data in the target path and performs incremental loading.
        """
        existing: Optional[DataFrame] = None
        # Attempt to read existing parquet data from the target_path directory
        try:
            # Assuming target_path points to the directory where parquet files are stored.
            existing = spark.read.parquet(target_path)
            L.info(f"Successfully read existing Parquet data from {target_path}. Row count: {existing.count()}")
        except Exception as e:
            L.warning(f"Could not read existing Parquet data from {target_path}: {e}. Proceeding without existing data.")

        if existing is not None and not existing.isEmpty():
            # Ensure the join column 'crash_record_id' exists in both DataFrames
            join_column = 'crash_record_id'
            if join_column in self.target.columns and join_column in existing.columns:
                # Perform the left_anti join to get only new records
                self.target = (self.target
                    .join(existing, on=join_column, how='left_anti'))
                L.info(f"Performed left_anti join with existing data on '{join_column}'. New records count: {self.target.count()}")
            else:
                L.warning(f"Cannot perform left_anti join: '{join_column}' not found in one or both dataframes. Loading all data from source.")

        else:
            L.info("No existing data to join with, or existing data is empty. Loading all data from source.")

    def load(self, target_path: str) -> None:
        """
        Writes the transformed data to a Parquet file in the specified target path.
        """
        self.target.write.mode('append').parquet(target_path)


def main(spark: SparkSession, args: Namespace) -> None:
    """
    Main entry point for the script.
    """
    api_url: str = args.api_url if args.api_url else Default.API_URL.value
    target_path: str = args.target_path if args.target_path else Default.TARGET_PATH.value

    crashes = Ingestor(spark, api_url, target_path)
