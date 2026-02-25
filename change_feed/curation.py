from argparse import Namespace
from enum import Enum
from logging import Logger
from typing import Optional

from pyspark.sql import DataFrame, DataFrameWriter, SparkSession

from pyspark.sql.column import Column
from pyspark.sql.functions import (
    col,
    to_timestamp,
    from_json,
    lit,
    to_date,
    sha1,
    struct,
    to_json,
)
from pyspark.sql.types import (
    ArrayType,
    StructType,
    StructField,
    StringType,
    IntegerType,
    TimestampType,
    DoubleType,
)


class Default(Enum):
    """
    Enumeration of default values
    """

    SOURCE_PATH = "/Volumes/workspace/google_drive/mock_s3"
    TARGET_PATH = "workspace.google_drive.silver_table"


class Location(Enum):
    """
    Enumeration of StructFields to select from the location column's JSON string values
    """

    SCHEMA = StructType(
        [
            StructField("type", StringType()),
            StructField("coordinates", ArrayType(DoubleType())),
        ]
    )


class Target(Enum):
    """
    Enumeration of target dataframe properties:
        - COLS: columns to curate from the bronze data
        - PRIMARY_KEY: primary key column name
        - LIQUID_KEYS: cluster columns
    """

    COLS = (
        col("computed_region_rpca_8um6"),
        col("alignment"),
        col("beat_of_occurrence"),
        to_date(to_timestamp(col("crash_date"), "yyyy-MM-dd'T'HH:mm:ss.SSS")).alias(
            "crash_date"
        ),
        to_timestamp(col("crash_date"), "yyyy-MM-dd'T'HH:mm:ss.SSS").alias(
            "crash_timestamp"
        ),
        col("crash_day_of_week").cast(IntegerType()),
        col("crash_hour").cast(IntegerType()),
        col("crash_month"),
        col("crash_year"),
        col("crash_record_id"),
        col("crash_type"),
        col("damage"),
        to_date(
            to_timestamp(col("date_police_notified"), "yyyy-MM-dd'T'HH:mm:ss.SSS")
        ).alias("date_police_notified"),
        to_timestamp(col("date_police_notified"), "yyyy-MM-dd'T'HH:mm:ss.SSS").alias(
            "timestamp_police_notified"
        ),
        col("device_condition"),
        col("first_crash_type"),
        col("injuries_fatal").cast(IntegerType()),
        col("injuries_incapacitating").cast(IntegerType()),
        col("injuries_no_indication").cast(IntegerType()),
        col("injuries_non_incapacitating").cast(IntegerType()),
        col("injuries_reported_not_evident").cast(IntegerType()),
        col("injuries_total").cast(IntegerType()),
        col("injuries_unknown").cast(IntegerType()),
        col("latitude").cast(DoubleType()),
        col("lighting_condition"),
        from_json(col("location"), Location.SCHEMA.value).alias("location"),
        col("longitude").cast(DoubleType()),
        col("most_severe_injury"),
        col("num_units").cast(IntegerType()),
        col("posted_speed_limit").cast(IntegerType()),
        col("prim_contributory_cause"),
        col("report_type"),
        col("road_defect"),
        col("roadway_surface_cond"),
        col("sec_contributory_cause"),
        col("street_direction"),
        col("street_name"),
        col("street_no"),
        col("traffic_control_device"),
        col("trafficway_type"),
        col("weather_condition"),
        col("intersection_related_i"),
        col("statements_taken_i"),
        col("hit_and_run_i"),
        col("photos_taken_i"),
        col("private_property_i"),
        col("crash_date_est_i"),
        col("work_zone_i"),
        col("dooring_i"),
        col("ingest_date"),
    )
    PRIMARY_KEY = "crash_record_id"
    LIQUID_KEYS = (
                    "report_type",
                    "crash_type",
                    "crash_date",
                    "crash_record_id"
                   )


class Curator:
    """
    Curator class
    """

    spark: SparkSession
    logger: Logger
    source_path: str
    target_path: str
    run_id: str
    source: DataFrame
    target_exists: bool

    def __init__(
        self,
        spark: SparkSession,
        logger: Logger,
        source_path: str,
        target_path: str,
        run_id: str,
    ) -> None:
        """
        constructor
        """
        self.spark = spark
        self.logger = logger
        self.source_path = source_path
        self.target_path = target_path
        self.run_id = run_id
        self.target_exists = self.spark.catalog.tableExists(self.target_path)
        self.run()

    def run(self) -> None:
        """
        run the pipeline using the following steps:
            - extract bronze data
            - transform bronze data into the silver table
            - load silver table to delta format
        """
        self.extract()
        self.transform(*Target.COLS.value)
        self.load()

    def extract(self) -> None:
        """
        extract bronze data
        """
        self.source = self.spark.read.parquet(self.source_path)

    def transform(self, *cols: Column) -> None:
        """
        transform bronze data into the silver table with the following columns:
            - cols: columns to curate from the bronze data
            - group_id: hash of the report_type, crash_type, and crash_date
            - run_id: run_id
        :param cols: columns to curate from the bronze data
        """
        self.source = (
            self.source.select(*cols)
            .withColumn(
                "group_id",
                sha1(to_json(struct("report_type", "crash_type", "crash_date"))),
            )
            .withColumn("run_id", lit(self.run_id))
        )

        if self.target_exists:
            target: DataFrame = self.spark.read.table(self.target_path)
            self.source = self.source.join(
                target, Target.PRIMARY_KEY.value, "left_anti"
            )

        self.logger.info(
            f"keeping {self.source.count()} records"
        )

    def load(self) -> None:
        """
        write silver table to delta format using the following properties:
            - mode: append
            - clusterBy: Target.LIQUID_KEYS.value
        """
        self.logger.info(f"writing silver table to {self.target_path}")
        
        writer: DataFrameWriter = self.source.write.format("delta")        
        writer = writer.mode("append") if self.target_exists else writer.mode("overwrite").clusterBy(*Target.LIQUID_KEYS.value)
            
        writer.saveAsTable(self.target_path)


def main(spark: SparkSession, logger: Logger, args: Namespace) -> None:
    """
    Instantiate the `Curator` class, using `args: Namespace` provided by `entry` point
    """
    assert args.run_id, "--run_id is required."

    Curator(
        spark=spark,
        logger=logger,
        source_path=args.source_path if args.source_path else Default.SOURCE_PATH.value,
        target_path=args.target_path if args.target_path else Default.TARGET_PATH.value,
        run_id=args.run_id,
    )
