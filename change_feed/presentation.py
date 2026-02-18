from argparse import Namespace
from enum import Enum
from logging import Logger
from typing import Dict

from delta.tables import DeltaTable
from pyspark.sql import SparkSession, DataFrame, DataFrameWriter

from pyspark.sql.functions import sha1, to_json, struct, lit, col, count, sum, max
from pyspark.sql.types import DateType


class Target(Enum):
    """
    Target table properties
    """

    MERGE_CONDITION = "t.crash_year = s.crash_year AND t.crash_month = s.crash_month AND t.crash_date = s.crash_date"
    MATCH_CONDITION = "t.hash_key != s.hash_key"


class Config:
    """
    Configuration class for the notebook
    """

    def __init__(self, args: Namespace):
        self.source_table = args.source_path
        self.target_table = args.target_path
        self.run_id = args.run_id


class Presentor:
    """
    Presentor class for the notebook
    """

    def __init__(self, spark: SparkSession, logger: Logger, config: Config) -> None:
        self.config = config
        self.logger = logger
        self.spark = spark
        self.run()

    def run(self) -> None:
        """
        Read from curated delta table and apply any required
        - Filter to specific level of detail
        - Aggregation, summarization
        - Write to gold delta table
        """
        self.extract()
        self.transform()
        self.load()

    def extract(self) -> None:
        """
        Read from curated delta table
        """
        self.source = self.spark.read.table(self.config.source_table)

    def transform(self) -> None:
        """
        Apply any required
        - Aggregation, summarization
        """
        self.source = (
            self.source.groupBy(
                "crash_year",
                "crash_month",
                col("crash_date").cast(DateType()).alias("crash_date"),
            )
            .agg(
                count("*").alias("crash_records"),
                sum("injuries_fatal").alias("fatalities"),
                sum("injuries_total").alias("injuries"),
                max("ingest_date").alias("max_ingest_date"),
            )
            .withColumn(
                "hash_key",
                sha1(to_json(struct("crash_records", "fatalities", "injuries"))),
            )
            .withColumn("update_run_id", lit(self.config.run_id))
        )

    def load(self) -> None:
        """
        Write to gold delta table
        """
        if self.spark.catalog.tableExists(self.config.target_table):
            target = DeltaTable.forName(self.spark, self.config.target_table)
            self.logger.info("Target table exists. Performing merge.")
            merge_metrics: DataFrame = (
                target.alias("t")
                .merge(self.source.alias("s"), Target.MERGE_CONDITION.value)
                .whenNotMatchedInsertAll()
                .whenMatchedUpdateAll(Target.MATCH_CONDITION.value)
                .execute()
            )
            [
                self.logger.info("{}: {}".format(k, v))
                for k, v in merge_metrics.collect()[0].asDict().items()
            ]
        else:
            self.logger("Target table does not exist. Creating new table")
            writer: DataFrameWriter = (
                self.source.write.format("delta")
                .mode("overwrite")
                .option("enableChangeDataFeed", "true")
            )
            writer.saveAsTable(self.config.target_table)


def main(spark: SparkSession, logger: Logger, args: Namespace) -> None:
    """
    Instantiate the Presentor class
    """
    assert args.source_path, "Source path is required"
    assert args.target_path, "Target path is required"
    assert args.run_id, "Run id is required"

    Presentor(spark=spark, logger=logger, config=Config(args))
