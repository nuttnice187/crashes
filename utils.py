from logging import Logger

from pyspark.testing import assertSchemaEqual

from pyspark.sql.types import StructType


def catch_schema_mismatch(
    source_schema: StructType, target_schema: StructType, logger: Logger
) -> None:
    """
    Catch schema mismatch and log a warning.
    """

    try:
        assertSchemaEqual(source_schema, target_schema)
        logger.info(f"Schemas match.")
    except AssertionError as e:
        logger.warning(
            f"Schema mismatch detected: {e}. Source schema: {source_schema}, Target schema: {target_schema}. Attempting to merge schemas."
        )
