#!/usr/bin/env python
"""Ticker data downloader"""

import asyncio
from loader import DataCenter
from datetime import datetime, timedelta
import struct
import csv
import click
import pystore
import dask
import pandas as pd


PYSTORE_FILE = "~/pystore"

def _convert(hour, tick):
    t = hour + timedelta(microseconds=tick[0])


async def download(ticker: str, days: int = 1):
    pystore.set_path(PYSTORE_FILE)
    store = pystore.store("FX")
    collection = store.collection(ticker)

    ct = DataCenter(timeout=30, use_cache=True)

    now = datetime.now()
    now = datetime(now.year,now.month,now.day)
    from_date = now - timedelta(days=days)

    hour = from_date
    while hour <= now:
        stream = await ct.get_ticks(ticker, hour)
        out = struct.iter_unpack(ct.format, stream.read())

        data = [[hour + timedelta(microseconds=tick[0]), tick[1], tick[2], tick[3], tick[4]] for tick in out]
        df = pd.DataFrame(columns=['time', 'a', 'b', 'c', 'd'], data=data)
        

        collection.append(, df)
        #    print(hour, tick[0], tick[1], tick[2])
        #    break
            #collection.write(ticker, aapl[:100], metadata={'source': 'dukascopy'})
        print(t)

        hour += timedelta(hours=1)

    timerange = (from_date, now)

    print(timerange)
    print(ticker)
    #generator = await ct.get_ticks_range(ticker, timerange)

    #for hour, stream in generator:
    #    print(f'Unpacking data for: {hour}')
    #    out = struct.iter_unpack(ct.format, stream.read())
    #    for tick in out:
    #        print(hour, tick[0], tick[1], tick[2])

@click.command()
@click.option("--days", default=3, help="Number of days to download.")
@click.option("--pair", default="EURUSD", help="Pair to download.")
def run(days: int, pair: str):
    """Download data"""
    asyncio.run(download(ticker=pair, days=days))

if __name__ == '__main__':
    run()