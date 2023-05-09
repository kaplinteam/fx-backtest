#!/usr/bin/env python
"""
Test playgorund
"""

import asyncio
import csv
import gzip
import struct
import pandas as pd
from dask import dataframe as dd
from datetime import datetime, timedelta
from prefect import flow, task, get_run_logger
from loader import DataCenter


@task
def load_duckastopy_to_gzip(ticker: str, day: datetime):
    """Download tick data for a single and store to gzip"""

    async def download_to_csv(ticker: str, day: datetime, writer_fn=None):
        """Download data & store it to compressed CSV file"""
        ct = DataCenter(timeout=30, use_cache=True)

        hour = datetime(day.year, day.month, day.day)
        to_date = hour + timedelta(days=1)

        while hour <= to_date:
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

        asyncio.run(download_to_csv(ticker=ticker, day=day, writer_fn=_tmp))


@task
def gzip_to_storage(ticker: str):
    """Load gzip into storage"""

    logger = get_run_logger()

    df = pd.read_csv(
        f"{ticker}.csv.gz",
        blocksize=None,
        header=None,
        names=["timestamp", "bid", "ask", "bid_size", "ask_size"],
        parse_dates=["timestamp"],
    )
    df = df.set_index("timestamp", sorted=True)
    df["bid"] = df["bid"] * 0.00001
    df["ask"] = df["ask"] * 0.00001

    if df.size.compute() == 0:
        return

    # Store it
    df.to_parquet("folder-fastparquet", engine="fastparquet", append=True)

    collection = _pystore_collection()
    if ticker in collection.items:
        collection.append(ticker, df)
        logger.info("Added %d records" % (len(df)))
    else:
        collection.write(ticker, df, metadata={"source": "Dukascopy"})
        logger.info("Newly created with %d records" % (len(df)))

@task
def pystore_export_to_digitalocean_space(ticker: str):
    """Export pystore to DO space"""
    pass

@flow(name="EURUSD data upgrade", log_prints=True)
def load_tickers(ticker: str, days: int):
    """Load ticker history"""

    now = datetime.now()

    for day in range(days, 0, -1):
        load_duckastopy_to_gzip(ticker=ticker, day=now - timedelta(days=day))
        gzip_to_storage(ticker)
    pystore_export_to_digitalocean_space(ticker)


load_tickers(ticker="EURUSD", days=10)  # 365 * 10)
