"""Contains functionality related to Weather"""
import json
import logging
from dataclasses import dataclass

logger = logging.getLogger(__name__)


class Weather:
    """Defines the Weather model"""

    def __init__(self):
        """Creates the weather model"""
        self.temperature = 70.0
        self.status = "sunny"

    def process_message(self, message):
        """Handles incoming weather data"""
        logger.info(f"Processing message with value={message.value()}")
        try:
            self.temperature = message.value().get('temperature')
            self.status = message.value().get('status')
        except Exception as e:
            logger.error(e)
