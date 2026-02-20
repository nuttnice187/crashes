from argparse import Namespace
from enum import Enum
from logging import Logger

from delta.tables import DeltaTable
from pyspark.sql import SparkSession, DataFrame, DataFrameWriter

from pyspark.sql.functions import (
    lit,
    col,
    count,
    sum,
    max,
)
from pyspark.sql.types import DateType


class Target(Enum):
    """
    Target table properties
    """

    ON_COLS = "t.id = s.id"
    PARTITION = "crash_year"
    UPDATE = {
        "crash_records": "t.crash_records + s.crash_records",
        "fatalities": "t.fatalities + s.fatalities",
        "injuries": "t.injuries + s.injuries",
        "ingest_date": "s.ingest_date",
        "run_id": "s.run_id",
    }


class Config:
    """
    Configuration class
    """

    source_table: str
    target_table: str
    run_id: str

    def __init__(self, args: Namespace) -> None:
        self.source_table = args.source_path
        self.target_table = args.target_path
        self.run_id = args.run_id


class Presentor:
    """
    Presentor class
    """

    spark: SparkSession
    logger: Logger
    config: Config
    source: DataFrame

    def __init__(self, spark: SparkSession, logger: Logger, config: Config) -> None:
        self.config = config
        self.logger = logger
        self.spark = spark
        self.run()

    def run(self) -> None:
        """
        - Read from silver delta table
        - Aggregation, summarization
        - Write to gold delta table
        """
        self.extract()
        self.transform()
        self.load()

    def extract(self) -> None:
        """
        Read from silver delta table
        - Filter by run_id
        """
        self.source = self.spark.read.table(self.config.source_table).filter(
            col("run_id") == self.config.run_id
        )

    def transform(self) -> None:
        """
        Apply any required
        - Aggregation, summarization
        """
        self.source = (
            self.source.groupBy(
                col("group_id").alias("id"),
                "report_type",
                "crash_type",
                "crash_date",
                "crash_year",
                "crash_month",
                "crash_day_of_week",
            )
            .agg(
                count("*").alias("crash_records"),
                sum("injuries_fatal").alias("fatalities"),
                sum("injuries_total").alias("injuries"),
                max("ingest_date").alias("ingest_date"),
            )
            .withColumn("run_id", lit(self.config.run_id))
        )

    def load(self) -> None:
        """
        Write to gold delta table
        - merge if table exists
        - overwrite if table does not exist

        """
        if self.spark.catalog.tableExists(self.config.target_table):
            self.logger.info("Target table exists. Performing merge.")
            self.merge()
        else:
            self.logger("Target table does not exist. Creating new table")
            self.overwrite()

    def merge(self) -> None:
        """
        merge
        """
        target: DeltaTable = DeltaTable.forName(self.spark, self.config.target_table)
        merge_metrics: DataFrame = (
            target.alias("t")
            .merge(self.source.alias("s"), condition=Target.ON_COLS.value)
            .whenNotMatchedInsertAll()
            .whenMatchedUpdate(set=Target.UPDATE.value)
            .execute()
        )
        [
            self.logger.info("{}: {}".format(k, v))
            for k, v in merge_metrics.collect()[0].asDict().items()
        ]

    def overwrite(self) -> None:
        """
        overwrite
        """
        writer: DataFrameWriter = (
            self.source.write.format("delta")
            .mode("overwrite")
            .partitionBy(Target.PARTITION.value)
            .option("enableChangeDataFeed", "true")
        )
        writer.saveAsTable(self.config.target_table)


def main(spark: SparkSession, logger: Logger, args: Namespace) -> None:
    """
    Instantiate the Presentor class
    """
    assert args.source_path, "--source_path is required"
    assert args.target_path, "--target_path is required"
    assert args.run_id, "--run_id is required"

    Presentor(spark=spark, logger=logger, config=Config(args))
