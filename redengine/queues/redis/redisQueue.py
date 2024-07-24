import abc
from redis import Redis
import asyncio

import aioredis
import async_timeout
import aioredis
import json
import datetime
STOPWORD = "STOP"

class IRedisQueue(abc.ABC):

    @abc.abstractmethod
    def subscribe_queue(self):
        pass


class RedisQueue(IRedisQueue):
    red = aioredis.from_url("redis://localhost")

    def __init__(self,
                 id: str = None,
                 ) -> None:
        self.id = id

    async def reader(self,channel: aioredis.client.PubSub):
        while True:
            try:
                async with async_timeout.timeout(0.01):
                    message = await channel.get_message(ignore_subscribe_messages=True)
                    if message is not None:
                        return json.loads(message['data'].decode())

                await asyncio.sleep(0.01)
            except asyncio.TimeoutError:
                pass

    async def subscribe_queue(self):
        # red = aioredis.from_url("redis://localhost")
        pubsub = self.red.pubsub()

        await pubsub.psubscribe(f"channel:{self.id}_recieve")
        future = asyncio.create_task(self.reader(pubsub))
        my_dict = {"id": f'{self.id}', "ewrerre": "ewrre"}
        ccc = json.dumps(my_dict)
        await self.red.publish(f"channel:{self.id}_ignite",ccc)
        await future

__all__ = ["RedisQueue"]
