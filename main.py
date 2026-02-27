import sys

from argparse import ArgumentParser, Namespace
from enum import Enum
from importlib import import_module
from logging import getLogger, INFO, Formatter, Logger, StreamHandler
from typing import Callable, Optional

from pyspark.sql import SparkSession


class Default(Enum):
    ROOT = "crashes"
    LOG_LEVEL = INFO


class JobTask:
    logger: Logger
    args: Namespace
    name: str
    root: str

    def __init__(self):
        self.parse_argv()
        self.get_name()
        self.get_logger()

    def get_logger(self) -> None:
        """
        Get logger
        Args:
            name (str): logger name
        Returns:
            Logger
        """

        level = int(self.args.log_level) if self.args.log_level else Default.LOG_LEVEL.value

        logger: Logger = getLogger(__name__)
        console_handler = StreamHandler()
        formatter = Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

        logger.setLevel(level)
        console_handler.setLevel(level)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

        self.logger = logger

    def parse_argv(self) -> None:
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
        parser.add_argument(
            "--run_id", type=str, help="databricks metadata for job run."
        )
        parser.add_argument("--log_level", type=str, help="Log level to use.")
        parser.add_argument("--root", type=str, help="Root directory.")

        self.args, unknown = parser.parse_known_args()

    def get_name(self) -> None:
        """
        Get job task from command line arguments
        Returns:
            str: job task
        """

        assert self.args.job, "--job is required."
        assert self.args.task, "--task is required."

        self.name: str = "{package}.{module}".format(
            package=self.args.job, module=self.args.task
        )

    def get_main_process(self) -> Callable[[SparkSession, Logger, Namespace], None]:
        """
        Get main process from command line arguments
        Returns:
            Callable[[SparkSession, Logger, Namespace], None]: main process
        """

        self.root = self.args.root if self.args.root else Default.ROOT.value

        if self.root in sys.path:
            self.logger.info(f"'{self.root}' already in sys.path")
        else:
            sys.path.insert(0, self.root)
            self.logger.info(f"Added '{self.root}' to sys.path")

        return import_module(self.name).main

    def run(self) -> None:
        """
        Run job task
        """

        main = self.get_main_process()

        self.logger.info(self.args)
        main(SparkSession.builder.getOrCreate(), self.logger, self.args)


if __name__ == "__main__":
    job_task = JobTask()
    job_task.run()
