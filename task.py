#!/usr/bin/env python
"""
Test playgorund
"""

import asyncio
import csv
import gzip
import struct
from datetime import datetime, timedelta

import click
import dask.dataframe as dd
from prefect import flow, task

import pystore
from loader import DataCenter


@task
def load_duckastopy_to_gzip(ticker: str, days: int):

    async def download_to_csv(ticker: str, days: int, writer_fn=None):
        """Download data & store it to compressed CSV file"""
        ct = DataCenter(timeout=30, use_cache=True)
        now = datetime.now()
        now = datetime(now.year, now.month, now.day)
        from_date = now - timedelta(days=days)

        hour = from_date
        while hour <= now:
            if hour.weekday() < 5:
                stream = await ct.get_ticks(ticker, hour)
                out = struct.iter_unpack(ct.format, stream.read())
                for tick in out:
                    tick = list(tick)
                    tick[0] = hour + timedelta(microseconds=tick[0])
                    if writer_fn is not None:
                        writer_fn(tick)
            hour += timedelta(hours=1)

    f = open(f"{ticker}.csv.gz", "wb")
    with gzip.open(f, "wt") as csvfile:
        datafile_writer = csv.writer(csvfile, quoting=csv.QUOTE_MINIMAL)

        def _tmp(row):
            datafile_writer.writerow(row)

        asyncio.run(download_to_csv(ticker=ticker, days=days, writer_fn=_tmp))

@task
def gzip_to_pystore(ticker: str):
    df = dd.read_csv(
        f"{ticker}.csv.gz",
        blocksize=None,
        header=None,
        names=["timestamp", "bid", "ask", "bid_size", "ask_size"],
        parse_dates=["timestamp"],
    )
    df = df.set_index("timestamp", sorted=True)
    df["bid"] = df["bid"] * 0.00001
    df["ask"] = df["ask"] * 0.00001

    # Store it
    pystore.set_path("pystore")
    store = pystore.store("ticks")
    collection = store.collection("FX")
    if ticker in collection.items:
        df = dd.concat([collection.item(ticker).data, df])
        df = df.reset_index()
        df = df.drop_duplicates(keep="last", subset="timestamp")
        df = df.set_index("timestamp", sorted=True)

    collection.write(ticker, df, metadata={"source": "Dukascopy"}, overwrite=True)

@flow(name="EURUSD data upgrade")
def main_flow(ticker: str, days: int):
    load_duckastopy_to_gzip(ticker, days)
    gzip_to_pystore(ticker)

print(main_flow(ticker="EURUSD", days=400))
