"""Defines trends calculations for stations"""
import configparser
import logging
from pathlib import Path

import faust


logger = logging.getLogger(__name__)
config = configparser.ConfigParser()
config.read(f"{Path(__file__).parents[1]}/config.ini")


# Faust will ingest records from Kafka in this format
class Station(faust.Record):
    stop_id: int
    direction_id: str
    stop_name: str
    station_name: str
    station_descriptive_name: str
    station_id: int
    order: int
    red: bool
    blue: bool
    green: bool


# Faust will produce records to Kafka in this format
class TransformedStation(faust.Record):
    station_id: int
    station_name: str
    order: int
    line: str


kafka_broker = config.get('env', 'kafka_bootstrap_servers').split(',')[0].replace('PLAINTEXT', 'kafka')
app = faust.App("stations-stream", broker=kafka_broker, store="memory://")
topic = app.topic("org.chicago.cta.stations", value_type=Station)
out_topic = app.topic("org.chicago.cta.stations.table.v1", value_type=TransformedStation, partitions=1)

table = app.Table(
    "stations",
    default=TransformedStation,
    partitions=1,
    changelog_topic=out_topic)


@app.agent(topic)
async def process_stations(stations):
    async for station in stations:
        if station.red:
            line = "red"
        elif station.blue:
            line = "blue"
        elif station.green:
            line = "green"
        else:
            line = "N/A"

        table[station.station_id] = TransformedStation(
            station_id=station.station_id,
            station_name=station.station_name,
            order=station.order,
            line=line
        )


if __name__ == "__main__":
    app.main()
