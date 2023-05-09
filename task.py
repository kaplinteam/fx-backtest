#!/usr/bin/env python
"""
Test playgorund
"""

import os
import csv
import gzip
import struct
import asyncio
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

    logger = get_run_logger()
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

    df = dd.read_csv(
        f"{ticker}.csv.gz",
        header=None,
        names=["timestamp", "bid", "ask", "bid_size", "ask_size"],
        blocksize=None,  # blocksize='10MB',
    )
    df["timestamp"] = dd.to_datetime(df["timestamp"], errors="coerce")
    df = df.dropna()
    if df.size.compute() == 0:
        return

    df["bid"] = df["bid"].apply(lambda x: x * 0.00001, meta=("bid", "float64"))
    df["ask"] = df["ask"].apply(lambda x: x * 0.00001, meta=("ask", "float64"))
    df = df.set_index("timestamp", sorted=True)

    # Create directory
    raw_path = f"storage/{ticker}_raw"
    if not os.path.exists(raw_path):
        os.mkdir(raw_path)

    # Store it to the RAW directory
    df.to_parquet(raw_path, engine="fastparquet", append=True, ignore_divisions=True)
    logger.info("Added %d records" % (len(df)))


@task
def storage_data_clean_and_optimize(ticker: str):
    """Data cleanup and deduplication"""

    logger = get_run_logger()

    raw_path = f"storage/{ticker}_raw"
    dist_path = f"storage/{ticker}"
    if not os.path.exists(dist_path):
        os.mkdir(dist_path)

    # Load & deduplicate
    df = dd.read_parquet(raw_path)
    df = df.reset_index()
    df = df.drop_duplicates(subset=["timestamp"], keep="last")
    df = df.set_index("timestamp", sorted=True)

    # Write results
    df.to_parquet(dist_path, engine="fastparquet", ignore_divisions=True)

    logger.info("Data file stored, %d records total" % (len(df)))


@flow(name="EURUSD data upgrade", log_prints=True)
def load_tickers(ticker: str, days: int):
    """Load ticker history"""

    now = datetime.now()

    for day in range(days, 0, -1):
        load_duckastopy_to_gzip(ticker=ticker, day=now - timedelta(days=day))
        gzip_to_storage(ticker)
    storage_data_clean_and_optimize(ticker)


load_tickers(ticker="EURUSD", days=500)
