from enum import Enum
from logging import Logger
from typing import Optional

from pyspark.sql import SparkSession

from pyspark.sql.functions import col, to_timestamp, from_json
from pyspark.sql.types import (
    ArrayType,
    StructType,
    StructField,
    StringType,
    IntegerType,
    TimestampType,
    DoubleType
)

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
        to_timestamp(col("crash_date"), "yyyy-MM-dd'T'HH:mm:ss.SSS").alias("crash_date"),
        col("crash_day_of_week").cast(IntegerType()),
        col("crash_hour").cast(IntegerType()),
        col("crash_month").cast(IntegerType()),
        col("crash_record_id"),
        col("crash_type"),
        col("damage"),
        to_timestamp(col("date_police_notified"), "yyyy-MM-dd'T'HH:mm:ss.SSS").alias("date_police_notified"),
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
        col("update_time")
        )


def main(
    spark: SparkSession, logger: Logger, source_path: str, target_path: Optional[str] = None
    ) -> None:
    """transform location"""
    source = spark.read.parquet(source_path)
    target = source.select(*Target.COLS.value)
    # target.write.mode("append").format("delta").save(target_path)