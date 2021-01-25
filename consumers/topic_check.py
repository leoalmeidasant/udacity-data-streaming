import configparser
from pathlib import Path

from confluent_kafka.admin import AdminClient

config = configparser.ConfigParser()
config.read(f"{Path(__file__).parents[1]}/config.ini")


def topic_exists(topic):
    """Checks if the given topic exists in Kafka"""
    client = AdminClient({"bootstrap.servers": config.get('env', 'kafka_bootstrap_servers')})
    topic_metadata = client.list_topics(timeout=5)
    return topic in set(t.topic for t in iter(topic_metadata.topics.values()))
