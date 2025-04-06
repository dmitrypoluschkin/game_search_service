import aioredis

REDIS_HOST = "redis"
REDIS_PORT = 6379

redis = None

async def init_redis():
    global redis
    redis = await aioredis.from_url(f"redis://{REDIS_HOST}:{REDIS_PORT}")

async def set_redis(key, value, ex=None):
    await redis.set(key, value, ex=ex)