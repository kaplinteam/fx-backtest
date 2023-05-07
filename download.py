#!/usr/bin/env python
"""Ticker data downloader"""

import asyncio
from loader import DataCenter
from datetime import datetime, timedelta
import struct
import csv
import click
import gzip


async def download(pair: str, days: int = 1):
    ct = DataCenter(timeout=30, use_cache=True)
    now = datetime.now()
    now = datetime(now.year, now.month, now.day)
    from_date = now - timedelta(days=days)

    f = open("data.csv.gz", "wb")
    with gzip.open(f, "wt") as csvfile:
        datafile_writer = csv.writer(
            csvfile, delimiter=" ", quotechar="|", quoting=csv.QUOTE_MINIMAL
        )
        hour = from_date
        while hour <= now:
            stream = await ct.get_ticks(pair, hour)
            out = struct.iter_unpack(ct.format, stream.read())
            ticks = 0
            for tick in out:
                tick = list(tick)
                tick[0] = hour + timedelta(microseconds=tick[0])
                datafile_writer.writerow(tick)
                ticks += 1
            print(f"Loaded {ticks} ticks at {hour} for {pair}")
            hour += timedelta(hours=1)


@click.command()
@click.option("--days", default=3, help="Number of days to download.")
@click.option("--pair", default="EURUSD", help="Pair to download.")
def run(days: int, pair: str):
    """Download data"""
    asyncio.run(download(pair=pair, days=days))


if __name__ == "__main__":
    run()
