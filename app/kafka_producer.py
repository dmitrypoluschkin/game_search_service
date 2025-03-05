import json
from aiokafka import AIOKafkaProducer
from typing import Optional

class KafkaProducer:
    def __init__(self):
        self.producer: Optional[AIOKafkaProducer] = None

    async def init(self):
        conf = {
            'bootstrap_servers': 'localhost:9092',
        }
        self.producer = AIOKafkaProducer(**conf)
        try:
            await self.producer.start()
        except Exception as e:
            await self.close()
            raise RuntimeError(f"Failed to start Kafka producer: {e}")

    async def close(self):
        if self.producer:
            try:
                await self.producer.stop()
            except Exception as e:
                raise RuntimeError(f"Failed to stop Kafka producer: {e}")

    async def send_message(self, topic: str, message: dict):
        try:
            encoded_message = json.dumps(message).encode('utf-8')
            await self.producer.send_and_wait(topic, encoded_message)
            print(f"Sent message to {topic}: {message}")
        except Exception as e:
            raise RuntimeError(f"Error sending message to Kafka: {e}")

    async def check_health(self):
        if not self.producer or self.producer._closed:
            return False
        return True
