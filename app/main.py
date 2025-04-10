from fastapi import FastAPI, HTTPException
from .kafka_producer import KafkaProducer
from .kafka_consumer import KafkaConsumer
from .database import async_session_maker
from .models import Game
from sqlalchemy.future import select
import asyncio
import json



app = FastAPI()

# Настройки Kafka
KAFKA_BROKERS = "kafka:9092"
KAFKA_CONSUMER_TOPIC = "search_topic"
KAFKA_PRODUCER_TOPIC = "response_topic"


producer = KafkaProducer()
consumer = KafkaConsumer(
    topic=KAFKA_CONSUMER_TOPIC,
    bootstrap_servers=KAFKA_BROKERS,
    group_id='game_service_group'
)


@app.on_event("startup")
async def startup_event():
    await producer.init()
    await consumer.start()
    asyncio.create_task(process_messages())


@app.on_event("shutdown")
async def shutdown_event():
    await producer.close()
    await consumer.stop()


async def process_messages():
    async for message in consumer.consume():
        query = message.get('query')
        
        async with async_session_maker() as session:
            result = await session.execute(select(Game).where(Game.name.ilike(f"%{query}%")))
            games = result.scalars().all()
        
        if games:
            response = [
                {
                "id": game.id,
                "name": game.name,
                "genre": game.genre,
                "release_year": game.release_year
            }
            for game in games
            ]
        else:
            response = {"error": "Game not found"}
        
        await producer.send_message(KAFKA_PRODUCER_TOPIC, response)


@app.post("/search")
async def search(query: str):
    async with async_session_maker() as session:
        result = await session.execute(select(Game).where(Game.name.ilike(f"%{query}%")))
        games = result.scalars().all()
    
    if games:
        return [
            {
            "id": game.id,
            "name": game.name,
            "genre": game.genre,
            "release_year": game.release_year
        }
        for game in games
        ]
    else:
        raise HTTPException(status_code=404, detail="Game not found")

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)