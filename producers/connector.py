"""Configures a Kafka Connector for Postgres Station data"""
import configparser
import logging
from pathlib import Path

import requests
from jinja2 import Template

import constant

logger = logging.getLogger(__name__)
config = configparser.ConfigParser()
config.read(f"{Path(__file__).parents[1]}/config.ini")


def _build_connector():
    template = Template(open('stations-connector.json.j2').read())
    return template.render(**config._sections['kafka_connect_cta'])


def configure_connector():
    """Starts and configures the Kafka Connect connector"""
    logging.info("creating or updating kafka connect connector...")

    connector = _build_connector()

    connect_url = f"{config.get('env', 'kafka_connect_uri')}/connectors/{config.get('kafka_connect_cta', 'name')}"
    resp = requests.get(connect_url)
    if resp.status_code == 200:
        logging.debug("connector already created, skipping recreation")
        return

    logger.debug("Kafka connector code working")
    resp = requests.post(
        f"{constant.KAFKA_CONNECT_URI}/connectors",
        headers={"Content-Type": "application/json"},
        data=connector,
    )

    # Ensure a healthy response was given
    resp.raise_for_status()
    logging.debug("connector created successfully")


if __name__ == "__main__":
    configure_connector()
