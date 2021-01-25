"""Configures a Kafka Connector for Postgres Station data"""
import json
import logging
import os

import requests

from jinja2 import Template
import constant

logger = logging.getLogger(__name__)


def _build_connector():
    template = Template(open('stations-connector.json.j2').read())
    rendered = template.render(jdbc_uri=constant.POSTGRES_JDBC_URI)
    return json.loads(rendered)


def configure_connector():
    """Starts and configures the Kafka Connect connector"""
    logging.info("creating or updating kafka connect connector...")

    connector = _build_connector()

    connect_url = f"{constant.KAFKA_CONNECT_URI}/connectors/{connector['name']}"
    resp = requests.get(connect_url)
    if resp.status_code == 200:
        logging.debug("connector already created, skipping recreation")
        return

    logger.debug("Kafka connector code working")
    resp = requests.post(
        f"{constant.KAFKA_CONNECT_URI}/connectors",
        headers={"Content-Type": "application/json"},
        data=json.dumps(connector),
    )

    # Ensure a healthy response was given
    resp.raise_for_status()
    logging.debug("connector created successfully")


if __name__ == "__main__":
    configure_connector()
