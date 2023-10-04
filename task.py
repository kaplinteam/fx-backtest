#!/usr/bin/env python
"""
Test playgorund
"""

import os
import csv
import gzip
import shutil
import struct
import asyncio
from dask import dataframe as dd
from datetime import datetime, timedelta
from prefect import flow, task, get_run_logger
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

from loader import DataCenter


def batched(iterable, n):
    "Batch data into tuples of length n. The last batch may be shorter."
    if n < 1:
        raise ValueError('n must be at least one')
    it = iter(iterable)
    while (batch := tuple(islice(it, n))):
        yield batch


@task(name="dukascopy", 
    task_run_name="dukascopy-{ticker}-on-{hours[0]:%c}",
    description="This task loads data from dukascopy & pushes it to the influxdb.")
def load_hours_data(ticker: str, hours: list[datetime]):
    """Load data for a hours"""

    hour = []
    url = os.environ.get("INFLUXDB_HOST", "http://localhost:8086")
    org = os.environ.get("INFLUXDB_ORG", "org")
    bucket = os.environ.get("INFLUXDB_BUCKET", "DUKASCOPY")
    token = os.environ.get("INFLUXDB_TOKEN")

    with InfluxDBClient(url=url, token=token, org=org, debug=False) as client:
        write_api = client.write_api(write_options=SYNCHRONOUS)

        async def download_to_csv(ticker: str, hours: datetime):
            """Download data & store it to compressed CSV file"""
            data_center = DataCenter(timeout=30, use_cache=False, threads=3, progress=False)

            tuples = await data_center.get_ticks_hours(symbol=ticker, hours=hours)
            tuples.sort(key=lambda x: x[0])
            for tupl in tuples:
                hour, stream = tupl
                rows = []
                for row in struct.iter_unpack(data_center.format, stream.read()):
                    row = list(row)
                    ts = int((hour + timedelta(microseconds=row[0])).timestamp() * 1000)
                    line = f'{ticker} bid={row[1]},ask={row[2]},bidSize={round(row[3], 4)},askSize={round(row[4], 4)} {ts}'
                    rows.append(line)
                write_api.write(bucket=bucket, record=rows, write_precision=WritePrecision.MS)

        asyncio.run(download_to_csv(ticker=ticker, hours=hours))


@flow(name="Loading tickers for last N days", log_prints=True)
def load_last_days_depth(tickers: list[str], days: int):
    """Load ticker history"""

    now = datetime.now()
    now = datetime(now.year, now.month, now.day)

    for day in range(days, 0, -1):
        day = now - timedelta(days=day)
        if day.weekday() < 5:
            for ticker in tickers:
                load_hours_data(ticker=ticker, hours=[day+timedelta(hours=i) for i in range(0, 24)])


@flow(name="Loading tickers for bays between from_days to to_days ago", log_prints=True)
def load_days_between_depth(tickers: list[str], from_days: int, to_days: int):
    """Load ticker history"""

    now = datetime.now()
    now = datetime(now.year, now.month, now.day)
    for day in range(from_days, to_days, -1):
        day = now - timedelta(days=day)
        if day.weekday() < 5:
            for ticker in tickers:
                load_hours_data(ticker=ticker, hours=[day+timedelta(hours=i) for i in range(0, 24)])


ALL_TICKERS = [
    "USA500IDXUSD",
    "USATECHIDXUSD",
    "LIGHTCMDUSD",
    "BRENTCMDUSD",
    "COPPERCMDUSD",
    "XPDCMDUSD",
    "XPTCMDUSD",
    "XAUUSD",
    "XAGUSD"]

load_days_between_depth(from_days=500, to_days=100, tickers=ALL_TICKERS)
