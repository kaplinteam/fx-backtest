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


@task(name="dukascopy", 
    task_run_name="dukascopy-{ticker}-on-{hour:%A}",
    description="This task loads data from dukascopy & pushes it to the influxdb.")
def load_hour_data(ticker: str, hour: datetime):
    """Load data for a hour"""

    url = os.environ.get("INFLUXDB_HOST", "http://localhost:8086")
    org = os.environ.get("INFLUXDB_ORG", "org")
    bucket = os.environ.get("INFLUXDB_BUCKET", "DUKASCOPY")
    token = os.environ.get("INFLUXDB_TOKEN")

    with InfluxDBClient(url=url, token=token, org=org, debug=False) as client:
        write_api = client.write_api(write_options=SYNCHRONOUS)

        async def download_to_csv(ticker: str, hour: datetime):
            """Download data & store it to compressed CSV file"""
            ct = DataCenter(timeout=30, use_cache=False)
            stream = await ct.get_ticks(ticker, hour)
            out = struct.iter_unpack(ct.format, stream.read())
            rows = []
            for row in out:
                row = list(row)
                ts = int((hour + timedelta(microseconds=row[0])).timestamp() * 1000)
                line = f'{ticker} bid={row[1]},ask={row[2]},bidSize={round(row[3], 4)},askSize={round(row[4], 4)} {ts}'
                rows.append(line)

            write_api.write(bucket=bucket, record=rows, write_precision=WritePrecision.MS)


        asyncio.run(download_to_csv(ticker=ticker, hour=hour))

    return


@flow(name="Loading tickers for last N days", log_prints=True)
def load_last_days_depth(tickers: list[str], days: int):
    """Load ticker history"""

    now = datetime.now()
    now = datetime(now.year, now.month, now.day)

    for ticker in tickers:
        for day in range(days, 0, -1):
            day = now - timedelta(days=day)
            if day.weekday() < 5:
                for i in range(0, 24):
                    load_hour_data(ticker=ticker, hour=day+timedelta(hours=i))


load_last_days_depth(days=30, tickers=[
    "USA500IDXUSD",
    "USATECHIDXUSD",
    "LIGHTCMDUSD",
    "BRENTCMDUSD",
    "COPPERCMDUSD",
    "XPDCMDUSD",
    "XPTCMDUSD",
    "XAUUSD",
    "XAGUSD"])
