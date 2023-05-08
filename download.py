#!/usr/bin/env python
"""Ticker data downloader"""

import asyncio
import csv
import gzip
import struct
from datetime import datetime, timedelta

import click

from loader import DataCenter


async def download_to_csv(pair: str, days: int = 1, writer_fn=None):
    """Download data & store it to compressed CSV file"""
    ct = DataCenter(timeout=30, use_cache=True)
    now = datetime.now()
    now = datetime(now.year, now.month, now.day)
    from_date = now - timedelta(days=days)

    hour = from_date
    while hour <= now:
        if hour.weekday() < 5:
            stream = await ct.get_ticks(pair, hour)
            out = struct.iter_unpack(ct.format, stream.read())
            for tick in out:
                tick = list(tick)
                tick[0] = hour + timedelta(microseconds=tick[0])
                if writer_fn is not None:
                    writer_fn(tick)
        hour += timedelta(hours=1)


@click.command()
@click.option("--days", default=1, help="Number of days to download.")
@click.option("--pair", default="EURUSD", help="Pair to download.")
def run(days: int = 1, pair: str = "EURUSD"):
    """Download data"""

    f = open(f"{pair}.csv.gz", "wb")
    with gzip.open(f, "wt") as csvfile:
        datafile_writer = csv.writer(csvfile, quoting=csv.QUOTE_MINIMAL)

        def _tmp(row):
            datafile_writer.writerow(row)

        asyncio.run(download_to_csv(pair=pair, days=days, writer_fn=_tmp))


if __name__ == "__main__":
    run()
