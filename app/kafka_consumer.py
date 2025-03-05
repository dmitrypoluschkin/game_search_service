import json
from aiokafka import AIOKafkaConsumer

class KafkaConsumer:
    def __init__(self, topic: str, bootstrap_servers: str, group_id: str):
        self.consumer = AIOKafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            auto_offset_reset='earliest'
        )
        self._is_running = True

    async def start(self):
        await self.consumer.start()

    async def stop(self):
        self._is_running = False
        await self.consumer.stop()

    async def consume(self):
        try:
            async for msg in self.consumer:
                if not self._is_running:
                    break

                message = json.loads(msg.value.decode('utf-8'))
                print(f"Received message: {message}")
                yield message
        except Exception as e:
            print(f"Error consuming messages: {e}")
        finally:
            await self.stop()
