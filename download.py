#!/usr/bin/env python
"""Ticker data downloader"""

import sys
import csv
import gzip

import asyncio
import click
import struct
from loguru import logger
from itertools import islice
from datetime import datetime, timedelta
import influxdb_client, os, time
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


async def download_to_csv(
    pair: str,
    hours: list[datetime],
    writer_fn = None,
    use_cache: bool = True,
    threads: int = 3,
):
    """Download data & store it to compressed CSV file"""
    data_center = DataCenter(timeout=30, use_cache=use_cache, threads=threads)

    tuples = await data_center.get_ticks_hours(symbol=pair, hours=hours)
    tuples.sort(key=lambda x: x[0])
    for tupl in tuples:
        hour, stream = tupl
        out = struct.iter_unpack(data_center.format, stream.read())
        for ticks in batched(out, 100):
            points = []
            for tick in ticks:
                tick = list(tick)
                tick[0] = hour + timedelta(milliseconds=tick[0])
                points.append(tick)
            if writer_fn:
                writer_fn(points)


@click.command()
@click.option("--cache", default=True, help="Use cache")
@click.option("--threads", default=3, help="Parallel threads")
@click.option("-v", is_flag=True, default=False, help="Verbose")
@click.option("--days", default=1, help="Number of days to download.")
@click.option("--date", default=None, help="Specific date to load (YYYY-MM-DD).")
@click.option("--pair", default="EURUSD", help="Pair to download.")
@click.option("--influx", default=None, help="Influx db bucket to store data to (INFLUXDB_TOKEN should be set)")
def run(
    cache: bool = True,
    threads: int = 3,
    v: bool = False,
    days: int = 1,
    date: str = None,
    pair: str = "EURUSD",
    influx: str = False,
):
    """Download data"""

    if not v:
        logger.remove()
        logger.add(sys.stderr, level="WARNING")

    if threads < 1:
        click.echo("Incorrectn umber of threads")

    hours_to_load = []
    if date != None:
        base = datetime.strptime(date, "%Y-%m-%d")
        for i in range(0, 24):
            hours_to_load.append(base + timedelta(hours=i))
    else:
        now = datetime.now()
        now = datetime(now.year, now.month, now.day)
        hour = now - timedelta(days=days)
        while hour < now:
            hours_to_load.append(hour)
            hour += timedelta(hours=1)

    # Remove non workign hours
    hours_to_load = [hour for hour in hours_to_load if hour.weekday() < 5]

    click.echo(f"Loading {pair}, {len(hours_to_load)} hours using {threads} threads")

    if influx is not None and len(influx) > 0:
        url = os.environ.get("INFLUXDB_HOST", "http://localhost:8086")
        org = os.environ.get("INFLUXDB_ORG", "org")
        token = os.environ.get("INFLUXDB_TOKEN")
        with InfluxDBClient(url=url, token=token, org=org, debug=False) as client:
            write_api = client.write_api(write_options=SYNCHRONOUS)

            def _writer(rows):
                points = [f'{pair} bid={row[1]},ask={row[2]},bidSize={round(row[3], 4)},askSize={round(row[4], 4)} {int(row[0].timestamp() * 1000)}' for row in rows]
                write_api.write(bucket=influx, record=points, write_precision=WritePrecision.MS)

            asyncio.run(
                download_to_csv(
                    pair=pair, hours=hours_to_load, writer_fn=_writer, use_cache=cache, threads=threads
                )
            )
    else:
        out_file = open(f"ticks_dukascopy_{pair}.csv.gz", "wb")
        with gzip.open(out_file, "wt") as csvfile:
            datafile_writer = csv.writer(csvfile, quoting=csv.QUOTE_MINIMAL)

            def _writer(rows):
                for row in rows:
                    datafile_writer.writerow(row)

            asyncio.run(
                download_to_csv(
                    pair=pair, hours=hours_to_load, writer_fn=_writer, use_cache=cache, threads=threads
                )
            )


if __name__ == "__main__":
    run()
