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


def all_hours(days: int = 1) -> list[datetime]:
    """Iterate on all hours"""
    now = datetime.now()
    now = datetime(now.year, now.month, now.day)
    from_date = now - timedelta(days=days)
    hour = from_date

    res = []
    while hour <= now:
        if hour.weekday() < 5:
            res.append(hour)
        hour += timedelta(hours=1)
    return res


async def download_to_csv(pair: str, days: int = 1, writer_fn=None):
    """Download data & store it to compressed CSV file"""
    data_center = DataCenter(timeout=30, use_cache=True)

    for hour in tqdm(all_hours(days=days)):
        stream = await data_center.get_ticks(pair, hour)
        out = struct.iter_unpack(data_center.format, stream.read())
        for tick in out:
            tick = list(tick)
            tick[0] = hour + timedelta(microseconds=tick[0])
            if writer_fn is not None:
                writer_fn(tick)


@click.command()
@click.option("--days", default=1, help="Number of days to download.")
@click.option("--pair", default="EURUSD", help="Pair to download.")
def run(days: int = 1, pair: str = "EURUSD"):
    """Download data"""

    logger.remove()
    logger.add(sys.stderr, level="ERROR")

    for each_pair in pair.strip().split(" "):
        if len(each_pair) == 0:
            continue

        click.echo(f"Loading {each_pair}")

        out_file = open(f"ticks_dukascopy_{each_pair}.csv.gz", "wb")
        with gzip.open(out_file, "wt") as csvfile:
            datafile_writer = csv.writer(csvfile, quoting=csv.QUOTE_MINIMAL)

            def _writer(row):
                datafile_writer.writerow(row)

            asyncio.run(download_to_csv(pair=each_pair, days=days, writer_fn=_writer))


if __name__ == "__main__":
    run()
