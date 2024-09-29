import asyncio

import async_timeout
import rethinkdb as r
import aioredis
import json
from redengine.configuring import Config
from polaroids.polaroids.tdk.prime import ActionPost

STOPWORD = "STOP"

rdb = r.RethinkDB()
conn = rdb.connect(host=Config.app.host, port=28015)


async def reader(channel: aioredis.client.PubSub):
    redis = await aioredis.from_url("redis://localhost")
    while True:
        try:
            async with async_timeout.timeout(0.01):
                message = await channel.get_message(ignore_subscribe_messages=True)
                if message is not None:
                    channel_id = json.loads(message["data"])
                    posts = ActionPost().get_post()
                    post_to_json = json.dumps(posts)
                    await redis.publish(
                        f"channel:{channel_id['id']}_recieve", post_to_json
                    )
                    if message["data"].decode() == STOPWORD:
                        break
                await asyncio.sleep(0.01)
        except asyncio.TimeoutError:
            pass


async def main():
    redis = await aioredis.from_url("redis://localhost")
    pubsub = redis.pubsub()
    await pubsub.psubscribe("channel:*ignite")

    future = asyncio.create_task(reader(pubsub))

    await future


if __name__ == "__main__":
    asyncio.run(main())
