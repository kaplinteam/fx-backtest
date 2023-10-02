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


@task
def load_hour_data(ticker: str, hour: datetime):
    """Load data for a hour"""

    url = os.environ.get("INFLUXDB_HOST", "http://localhost:8086")
    org = os.environ.get("INFLUXDB_ORG", "org")
    token = os.environ.get("INFLUXDB_TOKEN")

    with InfluxDBClient(url=url, token=token, org=org, debug=False) as client:
        write_api = client.write_api(write_options=SYNCHRONOUS)

        async def download_to_csv(ticker: str, hour: datetime):
            """Download data & store it to compressed CSV file"""
            ct = DataCenter(timeout=30, use_cache=True)
            stream = await ct.get_ticks(ticker, hour)
            out = struct.iter_unpack(ct.format, stream.read())
            rows = []
            for row in out:
                row = list(row)
                ts = int((hour + timedelta(microseconds=row[0])).timestamp() * 1000)
                line = f'{ticker} bid={row[1]},ask={row[2]},bidSize={round(row[3], 4)},askSize={round(row[4], 4)} {ts}'
                rows.append(line)

            write_api.write(bucket=influx, record=rows, write_precision=WritePrecision.MS)


        asyncio.run(download_to_csv(ticker=ticker, hour=hour))

    return


@flow(name="Loading ticker for last N days", log_prints=True)
def load_last_days(ticker: str, days: int):
    """Load ticker history"""

    now = datetime.now()
    for day in range(days, 0, -1):
        day = datetime(now.year, now.month, now.day) - timedelta(days=day)
        if day.weekday() < 5:
            for i in range(0, 24):
                load_hour_data(ticker=ticker, hour=day+timedelta(hours=i))


load_last_days(ticker="BRENTCMDUSD", days=10)
