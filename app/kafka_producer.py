from aiokafka import AIOKafkaProducer
from typing import Optional
import json
import asyncio
import logging
import os

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class KafkaProducer:
    def __init__(self):
        self.bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
        self.producer: Optional[AIOKafkaProducer] = None

    async def init(self):
        try:
            self.producer = AIOKafkaProducer(bootstrap_servers=self.bootstrap_servers)
            await self.producer.start()
            logger.info(f"Kafka producer initialized successfully with servers: {self.bootstrap_servers}")
        except Exception as e:
            await self.close()
            logger.error(f"Failed to start Kafka producer: {e}")
            raise RuntimeError(f"Failed to start Kafka producer: {e}")

    async def close(self):
        if self.producer:
            try:
                await self.producer.stop()
                logger.info("Kafka producer stopped successfully")
            except Exception as e:
                logger.error(f"Failed to stop Kafka producer: {e}")
                raise RuntimeError(f"Failed to stop Kafka producer: {e}")

    async def send_message(self, topic: str, message: dict):
        try:
            encoded_message = json.dumps(message).encode('utf-8')
            await self.producer.send_and_wait(topic, encoded_message, timeout=5)  # Добавлен таймаут
            logger.info(f"Sent message to {topic}: {message}")
        except Exception as e:
            logger.error(f"Error sending message to Kafka: {e}")
            raise RuntimeError(f"Error sending message to Kafka: {e}")

    async def check_health(self) -> bool:
        if not self.producer or self.producer._closed:
            logger.warning("Kafka producer is closed or not initialized")
            return False
        try:
            # Проверка доступности Kafka
            metadata = await self.producer.client.cluster.fetch_cluster_metadata()
            if metadata:
                logger.info("Kafka producer is healthy")
                return True
        except Exception as e:
            logger.error(f"Kafka health check failed: {e}")
        return False
