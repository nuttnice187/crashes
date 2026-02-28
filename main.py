import sys

from argparse import ArgumentParser, Namespace
from enum import Enum
from importlib import import_module
from logging import getLogger, INFO, Formatter, Logger, StreamHandler
from typing import Callable, Dict, Tuple, Union

from pyspark.sql import SparkSession


class Default(Enum):
    ROOT = "crashes"
    LOG_LEVEL = INFO
    ARG_CONFIGS = (
        ("--job", dict(type=str, help="Python package name.")),
        ("--task", dict(type=str, help="Python module name.")),
        ("--api_url", dict(type=str, help="API URL to fetch data from.")),
        ("--source_path", dict(type=str, help="Source path to read from.")),
        ("--target_path", dict(type=str, help="Target path to write to.")),
        ("--run_id", dict(type=str, help="databricks metadata for job run.")),
        ("--log_level", dict(type=str, help="Log level to use.")),
        ("--root", dict(type=str, help="Root directory."))
    )


class JobTask:
    logger: Logger
    args: Namespace
    name: str
    root: str

    def __init__(self, *arg_configs: Tuple[str, Dict[str, Union[type, str]]]) -> None:
        self.parse_argv(*arg_configs)
        self.set_name()
        self.set_logger()

    def parse_argv(self, *arg_configs: Tuple[str, Dict[str, Union[type, str]]]) -> None:
        """
        Parse command line arguments
        """

        parser = ArgumentParser(
            description="Ingest data from API into Spark and save as Parquet."
            "Curate silver delta table from ingested datasource."
            "Present gold delta table from curated change feed."
        )

        [parser.add_argument(arg, **kwargs) for arg, kwargs in arg_configs]

        self.args, unknown = parser.parse_known_args()

    def set_name(self) -> None:
        """
        Set job task from command line arguments
        """

        assert self.args.job, "--job is required."
        assert self.args.task, "--task is required."

        self.name = "{package}.{module}".format(
            package=self.args.job, module=self.args.task
        )

    def set_logger(self) -> None:
        """
        Set logger
        """

        level: int = int(self.args.log_level) if self.args.log_level else Default.LOG_LEVEL.value

        logger: Logger = getLogger(__name__)
        console_handler = StreamHandler()
        formatter = Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

        logger.setLevel(level)
        console_handler.setLevel(level)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

        self.logger = logger

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
    job_task = JobTask(*Default.ARG_CONFIGS.value)
    job_task.run()
