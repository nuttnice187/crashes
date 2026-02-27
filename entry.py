import sys

from argparse import ArgumentParser, Namespace
from importlib import import_module

from logging import getLogger, INFO, Formatter, Logger, StreamHandler
from typing import Callable

from pyspark.sql import SparkSession

ROOT: str = "crashes"


def get_logger(name: str) -> Logger:
    """
    Get logger
    Args:
        name (str): logger name
    Returns:
        Logger
    """

    logger: Logger = getLogger(name)
    console_handler = StreamHandler()
    formatter = Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    logger.setLevel(INFO)
    console_handler.setLevel(INFO)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    return logger


def parse_argv() -> Namespace:
    """
    Parse command line arguments
    Returns:
        Namespace: parsed arguments
    """

    parser = ArgumentParser(
        description="Ingest data from API into Spark and save as Parquet."
        "Curate silver delta table from ingested datasource."
        "Present gold delta table from curated change feed."
    )

    parser.add_argument("--job", type=str, help="Python package name.")
    parser.add_argument("--task", type=str, help="Python module name.")
    parser.add_argument("--api_url", type=str, help="API URL to fetch data from.")
    parser.add_argument("--source_path", type=str, help="Source path to read from.")
    parser.add_argument("--target_path", type=str, help="Target path to write to.")
    parser.add_argument("--run_id", type=str, help="databricks metadata for job run.")

    args, unknown = parser.parse_known_args()

    return args


def get_job_task(args: Namespace) -> str:
    """
    Get job task from command line arguments
    Returns:
        str: job task
    """

    assert args.job, "--job is required."
    assert args.task, "--task is required."

    return "{package}.{module}".format(package=args.job, module=args.task)


def get_main_process(
    root: str, job_task: str, logger: Logger
) -> Callable[[SparkSession, Logger, Namespace], None]:
    """
    Get main process from command line arguments
    Returns:
        Callable[[SparkSession, Logger, Namespace], None]: main process
    """

    if root in sys.path:
        logger.info(f"'{root}' already in sys.path")
    else:
        sys.path.insert(0, root)
        logger.info(f"Added '{root}' to sys.path")

    return import_module(job_task).main


if __name__ == "__main__":
    args: Namespace = parse_argv()
    job_task: str = get_job_task(args)
    logger: Logger = get_logger(__name__)
    main: Callable[[SparkSession, Logger, Namespace], None] = get_main_process(
        ROOT, job_task, logger
    )

    logger.info(args)
    main(SparkSession.builder.getOrCreate(), logger, args)
