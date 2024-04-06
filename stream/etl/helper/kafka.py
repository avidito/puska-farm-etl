from typing import Callable
from logging import Logger

import json
from kafka import KafkaConsumer

from etl.helper.config import CONFIG


class KafkaHelper:
    __label: str
    __topic: str
    __host: str
    __logger: Logger

    def __init__(self, label: str, logger: Logger):
        self.__label = label
        self.__logger = logger
        self.__topic = CONFIG.KAFKA_TOPIC
        self.__host = CONFIG.KAFKA_HOST
    

    # Methods
    def run(self, process: Callable, **kwargs):
        try:
            self.__logger.info(f"Starting - {self.__label}. Host: '{self.__host}' Topic: '{self.__topic}'")
            consumer = KafkaConsumer(
                self.__topic,
                bootstrap_servers = self.__host,
                value_deserializer = self.__value_deserializer,
            )

            for msg in consumer:
                if(msg.value):
                    process(msg.value, **kwargs)
        except KeyboardInterrupt:
            self.__logger.info(f"Closing - {self.__label}")
    

    # Private
    def __value_deserializer(self, data: bytes):
        try:
            return json.loads(data.decode("utf-8"))
        except json.decoder.JSONDecodeError:
            self.__logger.error("Failed to parse new data as JSON (dict)")
            return {}