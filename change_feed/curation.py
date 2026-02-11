from argparse import Namespace
from enum import Enum
from logging import Logger
from typing import Optional

from pyspark.sql import DataFrame, SparkSession

from pyspark.sql.column import Column
from pyspark.sql.functions import col, to_timestamp, from_json, lit
from pyspark.sql.types import (
    ArrayType,
    StructType,
    StructField,
    StringType,
    IntegerType,
    TimestampType,
    DoubleType
)

def check_existing(
    spark: SparkSession, source: DataFrame, target_path: str, primary_key: str
    ) -> DataFrame:
    """
    check existing
    """
    result = source
    if spark.catalog.tableExists(target_path):
        existing: DataFrame = spark.read.table(target_path)
        result = source.join(existing, primary_key, "left_anti")
    return result


class Default(Enum):
    SOURCE_PATH = "/Volumes/workspace/google_drive/mock_s3"
    TARGET_PATH = "workspace.google_drive.silver_table"

class Location(Enum):
    SCHEMA = StructType([
        StructField("type", StringType()),
        StructField("coordinates", ArrayType(DoubleType()))
    ])

class Target(Enum):
    COLS = (
        col("computed_region_rpca_8um6"),
        col("alignment"),
        col("beat_of_occurrence"),
        to_timestamp(col("crash_date"), 
                     "yyyy-MM-dd'T'HH:mm:ss.SSS").alias("crash_date"),
        col("crash_day_of_week").cast(IntegerType()),
        col("crash_hour").cast(IntegerType()),
        col("crash_month").cast(IntegerType()),
        col("crash_record_id"),
        col("crash_type"),
        col("damage"),
        to_timestamp(col("date_police_notified"), 
                     "yyyy-MM-dd'T'HH:mm:ss.SSS").alias("date_police_notified"),
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
        col("work_zone_type"),
        col("workers_present_i"),
        col("dooring_i"),
        col("ingest_time")
        )
    
class Curator:
    spark: SparkSession, 
    logger: Logger, 
    source_path: DataFrame,
    target_path: str,
    run_id: str
    source: DataFrame
    target: DataFrame
    def __init__(
            self,
            spark: SparkSession, 
            logger: Logger, 
            source_path: DataFrame,
            target_path: str,
            run_id: str
            ) -> None:
        self.spark = spark
        self.logger = logger
        self.source_path = source_path
        self.target_path = target_path
        self.run_id = run_id
        self.run()
    
    def run(self) -> None:
        self.extract(spark, source_path)
        self.transform(*Target.COLS.value)
        self.load()
    
    def extract(self, source_path: str) -> None:
        """
        extract bronze data
        """
        self.source: DataFrame = self.spark.read.parquet(source_path)
        
    def transform(self, *cols: Column) -> None:
        """
        transform bronze data into the silver table
        """
        self.target: DataFrame = check_existing(
            spark=self.spark, 
            source=self.source.select(*cols).withColumn(lit(self.run_id).alias("run_id")), 
            target_path=self.target_path, 
            primary_key="crash_record_id"
            )    
    
    def load(self) -> None:
        """
        write silver table
        """
        self.logger.info(f"writing silver table to {self.target_path}")
        self.target.write.mode("append").format("delta").save(self.target_path)
    

def main(
    spark: SparkSession, logger: Logger, args: Namespace
    ) -> None:
    """transform location"""
    Curator(
        spark=spark, 
        logger=logger, 
        source_path=args.source_path if args.source_path else Default.SOURCE_PATH.value, 
        target_path=args.target_path if args.target_path else Default.TARGET_PATH.value,
        run_id=args.run_id
    )