import logging
from datetime import timedelta
from typing import Any, Tuple, Type

import pandas as pd
import sqlalchemy as sa
from can_tools import ALL_SCRAPERS
from can_tools.scrapers.base import DatasetBase

import prefect
from prefect import Flow
from prefect.schedules import CronSchedule
from prefect.tasks.secrets import EnvVarSecret
from prefect.tasks.prefect.flow_run import StartFlowRun

_logger = logging.getLogger(__name__)


@prefect.task
def create_scraper(cls: Type[DatasetBase]) -> DatasetBase:
    logger = prefect.context.get("logger")
    dt = prefect.context.get("scheduled_start_time")
    logger.info("Creating class {} with dt = {}".format(cls, dt))
    return cls(execution_dt=dt)


@prefect.task(max_retries=3, retry_delay=timedelta(minutes=1))
def fetch(scraper: DatasetBase):
    logger = prefect.context.get("logger")
    method_name = f"{scraper.__class__.__name__}.fetch_and_store"
    logger.info(f"About to run {method_name}")
    logger.info(f"In fetch and have execution_dt = {scraper.execution_dt}")
    output_path = scraper.fetch_and_store()
    logger.info(f"{method_name} success")
    logger.info(f"Saved raw data to: {output_path}")


@prefect.task(max_retries=3, retry_delay=timedelta(minutes=1))
def normalize(scraper: DatasetBase):
    logger = prefect.context.get("logger")
    method_name = f"{scraper.__class__.__name__}.normalize_and_store"
    logger.info(f"About to run {method_name}")
    logger.info(f"In _normalize and have execution_dt = {scraper.execution_dt}")

    output_path = scraper.normalize_and_store()

    logger.info(f"{method_name} success")
    logger.info(f"Saved clean data to: {output_path}")


@prefect.task(max_retries=3, retry_delay=timedelta(minutes=1))
def validate(scraper: DatasetBase):
    is_valid = scraper.validate_normalized_data()
    if not is_valid:
        raise ValueError("failed validation")


@prefect.task(max_retries=3, retry_delay=timedelta(seconds=10))
def put(scraper: DatasetBase, connstr: str):
    logger = prefect.context.get("logger")

    engine = sa.create_engine(connstr)
    method_name = f"{scraper.__class__.__name__}.put_normalized_data"
    logger.info(f"About to run {method_name}")
    logger.info(
        f"In put_normalized_data and have execution_dt = {scraper.execution_dt}"
    )
    success, rows_in, rows_out = scraper.put_normalized_data(engine)

    logger.info(f"{method_name} success")
    logger.info(f"Inserted {rows_in} rows")
    logger.info(f"Deleted {rows_out} rows from temp table")

    return success


def create_flow_for_scraper(scraper_cls: Type[DatasetBase]):
    with Flow(scraper_cls.__name__) as flow:
        connstr = EnvVarSecret("COVID_DB_CONN_URI")
        scraper = create_scraper(scraper_cls)
        fetch_task = fetch(scraper)
        normalize_task = normalize(scraper)
        validate_task = validate(scraper)
        put_task = put(scraper, connstr)

        normalize_task.set_upstream(fetch_task)
        validate_task.set_upstream(normalize_task)
        put_task.set_upstream(validate_task)

    return flow


def create_master_flow():
    with Flow("MasterFlow") as flow:
        tasks = []
        for scraper_cls in ALL_SCRAPERS[:2]:
            task = StartFlowRun(
                flow_name=scraper_cls.__name__, project_name="can-scrape", wait=True
            )
            tasks.append(task)

        parquet_flow = StartFlowRun(
            flow_name="UpdateParquetFiles", project_name="can-scrape", wait=True
        )

        for task in tasks:
            task.set_downstream(parquet_flow)

    flow.register(project_name="can-scrape")


def register_all_scrapers():

    for scraper_cls in ALL_SCRAPERS:
        if not scraper_cls.autodag:
            continue
        _logger.info(f"Registering {scraper_cls.__name__}")
        flow = create_flow_for_scraper(scraper_cls)
        flow.register(project_name="can-scrape")


if __name__ == "__main__":
    register_all_scrapers()
    create_master_flow()
