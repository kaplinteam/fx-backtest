#!/usr/bin/env python
"""Ticker data downloader"""

import sys
import csv
import gzip

import asyncio
import click
import struct
from tqdm import tqdm
from datetime import datetime, timedelta

from loguru import logger
from loader import DataCenter


async def download_to_csv(pair: str, hours: list[datetime], writer_fn=None):
    """Download data & store it to compressed CSV file"""
    data_center = DataCenter(timeout=30, use_cache=True)

    tuples = await data_center.get_ticks_hours(symbol=pair, hours=hours)
    tuples.sort(key=lambda x: x[0])
    for tupl in tuples:
        hour, stream = tupl
        out = struct.iter_unpack(data_center.format, stream.read())
        for tick in out:
            tick = list(tick)
            tick[0] = hour + timedelta(microseconds=tick[0])
            if writer_fn is not None:
                writer_fn(tick)


@click.command()
@click.option("--days", default=1, help="Number of days to download.")
@click.option("--date", default=None, help="Specific date to load (YYYY-MM-DD).")
@click.option("--pair", default="EURUSD", help="Pair to download.")
def run(days: int = 1, date: str = None, pair: str = "EURUSD"):
    """Download data"""

    logger.remove()
    #logger.add(sys.stderr, level="ERROR")

    hours_to_load = []
    if date != None:
        base = datetime.strptime(date, '%Y-%m-%d')
        if base.weekday() < 5:
            for i in range(0, 24):
                hours_to_load.append(base + timedelta(hours=i))
        else:
            click.echo(f"Date is a weekend")
    else:
        now = datetime.now()
        now = datetime(now.year, now.month, now.day)
        hour = now - timedelta(days=days)
        while hour <= now:
            if hour.weekday() < 5:
                hours_to_load.append(hour)
            hour += timedelta(hours=1)

    click.echo(f"Loading {pair}, {len(hours_to_load)} hours")

    out_file = open(f"ticks_dukascopy_{pair}.csv.gz", "wb")
    with gzip.open(out_file, "wt") as csvfile:
        datafile_writer = csv.writer(csvfile, quoting=csv.QUOTE_MINIMAL)

        def _writer(row):
            datafile_writer.writerow(row)

        asyncio.run(download_to_csv(pair=pair, hours=hours_to_load, writer_fn=_writer))


if __name__ == "__main__":
    run()
