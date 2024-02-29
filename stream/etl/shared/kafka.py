import json
import logging
from kafka import KafkaConsumer


# Main
def get_stream_source(sequence_name: str, topic: str, host: str, process: callable, logger: logging.Logger):
    try:
        logger.info(f"Starting - {sequence_name}")
        consumer = KafkaConsumer(
            topic,
            value_deserializer = __value_deserializer(logger),
            bootstrap_servers = host
        )
        for msg in consumer:
            if(msg.value):
                process(msg.value)
    except KeyboardInterrupt:
        logger.info(f"Closing - {sequence_name}")


# Deserialzer
def __value_deserializer(logger: logging.Logger):
    def __wrapper(data: bytes):
        try:
            return json.loads(data.decode("utf-8"))
        except json.decoder.JSONDecodeError:
            logger.error("Failed to parse new data as JSON (dict)")
            return {}
    return __wrapper