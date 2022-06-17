#!/bin/python

from datetime import datetime, timedelta
from influxdb_client import InfluxDBClient, Point, WriteOptions
import random
import time
import os

INFLUX_URL=os.environ['INFLUX_URL']
INFLUX_TOKEN=os.environ['INFLUX_TOKEN']
INFLUX_ORG=os.environ['INFLUX_ORG']


with InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG) as _client:
    with _client.write_api(write_options=WriteOptions(batch_size=500,
                                                          flush_interval=1_000,
                                                          jitter_interval=2_000,
                                                          retry_interval=5_000,
                                                          max_retries=5,
                                                          max_retry_delay=30_000,
                                                          exponential_base=2)) as _write_client:
        while True:
            now = datetime.utcnow()

            for i in range(10):
                _write_client.write("water_level_raw", INFLUX_ORG, {
                    "measurement": "h2o_feet",
                    "tags": {"location": "coyote_creek", "i": i},
                    "fields": {"water_level": random.randrange(1,100)},
                    "time": now},
                )
            for i in range(10):
                late = now - timedelta(minutes=random.randrange(0,59))
                print(i, late)
                _write_client.write("water_level_raw", INFLUX_ORG, {
                    "measurement": "h2o_feet",
                    "tags": {"location": "coyote_creek", "i": i},
                    "fields": {"water_level": random.randrange(1,100)},
                    "time": late},
                )
            time.sleep(1)
