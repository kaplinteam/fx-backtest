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
from loader import DataCenter


@task
def load_hour_data(ticker: str, hour: datetime):
    """Load data for a hour"""

    url = os.environ.get("INFLUXDB_HOST", "http://localhost:8086")
    org = os.environ.get("INFLUXDB_ORG", "org")
    token = os.environ.get("INFLUXDB_TOKEN")

    with InfluxDBClient(url=url, token=token, org=org, debug=False) as client:
        write_api = client.write_api(write_options=SYNCHRONOUS)

        async def download_to_csv(ticker: str, day: datetime, writer_fn=None):
            """Download data & store it to compressed CSV file"""
            ct = DataCenter(timeout=30, use_cache=True)
            stream = await ct.get_ticks(ticker, hour)
            out = struct.iter_unpack(ct.format, stream.read())
            for tick in out:
                tick = list(tick)
                tick[0] = hour + timedelta(microseconds=tick[0])

                if writer_fn is not None:
                    writer_fn(tick)


        def _writer(rows):
            points = [f'{ticker} bid={row[1]},ask={row[2]},bidSize={round(row[3], 4)},askSize={round(row[4], 4)} {int(row[0].timestamp() * 1000)}' for row in rows]
            write_api.write(bucket=influx, record=points, write_precision=WritePrecision.MS)

        asyncio.run(
            download_to_csv(
                ticker=ticker, hours=hours_to_load, writer_fn=_writer, use_cache=cache, threads=threads
            )
        )

    return


@task
def load_day_data(ticker: str, day: datetime):
    """Load data for a day"""
    day_start = datetime(day.year, day.month, day.day)
    for i in range(0, 24):
        load_hour_data(ticker=ticker, hour=day_start+timedelta(hours=i))


@flow(name="Loading ticker for last N days", log_prints=True)
def load_last_days(ticker: str, days: int):
    """Load ticker history"""

    now = datetime.now()
    for day in range(days, 0, -1):
        day = now - timedelta(days=day)
        load_day_data(ticker=ticker, day=day)


load_last_days(ticker="EURUSD", days=14)

load_last_days(ticker="BRENTCMDUSD", days=10)
