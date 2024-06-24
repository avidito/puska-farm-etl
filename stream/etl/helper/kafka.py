import json
from logging import Logger
from typing import Callable, Type
from pydantic import BaseModel
from pydantic_core import ValidationError

from kafka import KafkaConsumer

from etl.helper.config import CONFIG


class KafkaHelper:
    __label: str
    __topic: str
    __uri: str
    __logger: Logger

    def __init__(self, label: str, logger: Logger):
        self.__label = label
        self.__logger = logger
        self.__topic = CONFIG.KAFKA_TOPIC
        self.__uri = f"{CONFIG.KAFKA_HOST}:{CONFIG.KAFKA_PORT}"
    

    # Methods
    def run(self, process: Callable, Validator: Type[BaseModel], **kwargs):
        try:
            consumer = KafkaConsumer(
                self.__topic,
                bootstrap_servers = self.__uri,
                value_deserializer = self.__value_deserializer,
            )
            self.__logger.info(f"Starting - {self.__label}. URI: '{self.__uri}' with TOPIC: '{self.__topic}'")

            for msg in consumer:
                try:
                    if (msg.value):
                        ev_data = Validator(**msg.value)
                        process(ev_data, **kwargs)
                except ValidationError as err:
                    self.__logger.error(err)

        except KeyboardInterrupt:
            self.__logger.info(f"Closing - {self.__label}")
    

    # Private
    def __value_deserializer(self, data: bytes):
        try:
            return json.loads(data.decode("utf-8"))
        except json.decoder.JSONDecodeError:
            self.__logger.error("Failed to parse new data as JSON (dict)")
            return {}
