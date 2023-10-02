"""Check connection"""
from itertools import batched

import influxdb_client, os, time
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

url = os.environ.get("INFLUXDB_HOST", "http://localhost:8086")
org = os.environ.get("INFLUXDB_ORG", "org")
token = os.environ.get("INFLUXDB_TOKEN")

client = InfluxDBClient(url=url, token=token, org=org)
write_api = client.write_api(write_options=SYNCHRONOUS)
query_api = client.query_api()

bucket="LIGHTCMDUSD"

def write_data_to_influx(client: InfluxDBClient, data: list, bucket: str, timeseries: str):
  write_api = client.write_api(write_options=SYNCHRONOUS)
  for values in batched(data, 100):

    points = [f'{timeseries} bid=123,ask=123,bidSize=123,askSize=123 1556813561098000000' for value in values]
    for point in points:
      print(point)
    break
    #write_api.write(bucket=bucket, org="org", record=point)
    #client.write_points(points, time_precision='ms')

