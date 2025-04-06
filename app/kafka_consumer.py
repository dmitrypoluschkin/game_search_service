from aiokafka import AIOKafkaConsumer
import asyncio
import logging
import os
import json

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class KafkaConsumer:
    def __init__(self, topic: str, group_id: str):
        self.bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        self.topic = topic
        self.group_id = group_id
        self.consumer = None

    async def start(self):
        try:
            self.consumer = AIOKafkaConsumer(
                self.topic,
                bootstrap_servers=self.bootstrap_servers,
                group_id=self.group_id,
                value_deserializer=lambda x: json.loads(x.decode("utf-8")),
            )
            await self.consumer.start()
            logger.info(f"Kafka consumer started for topic: {self.topic}")
        except Exception as e:
            logger.error(f"Failed to start Kafka consumer: {e}")
            raise RuntimeError(f"Failed to start Kafka consumer: {e}")

    async def stop(self):
        if self.consumer:
            try:
                await self.consumer.stop()
                logger.info("Kafka consumer stopped successfully")
            except Exception as e:
                logger.error(f"Failed to stop Kafka consumer: {e}")
                raise RuntimeError(f"Failed to stop Kafka consumer: {e}")

    async def consume(self):
        try:
            async for msg in self.consumer:
                yield msg.value
        except Exception as e:
            logger.error(f"Error consuming message from Kafka: {e}")
            raise RuntimeError(f"Error consuming message from Kafka: {e}")