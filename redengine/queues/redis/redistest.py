import asyncio
import async_timeout
import aioredis
import datetime

STOPWORD = "STOP"
red = aioredis.from_url("redis://localhost")


async def reader2(channel: aioredis.client.PubSub):
    while True:
        try:

            async with async_timeout.timeout(1):
                message = await channel.get_message(ignore_subscribe_messages=True)
                print(f"(Reader) Message Received: {message}")
                if message is not None:
                    feed = r.table("users").changes().run(conn)
                    for change in feed:
                        print(change)

                    await red.publish("channel:2", "444444444444444444444")
                    await red.publish("channel:2", STOPWORD)
                    print(f"(Reader) Message Received: {message}")
                    if message["data"].decode() == STOPWORD:
                        print("(Reader) STOP")
                        break
            await asyncio.sleep(5)
        except asyncio.TimeoutError:
            pass


async def pubPost():
    red = aioredis.from_url("redis://localhost")
    pubsub = red.pubsub()

    await pubsub.psubscribe("channel:*")
    print("3242342342342424234234==============")
    future = asyncio.create_task(reader2(pubsub))

    await future


async def main():
    tasks = []
    start = datetime.datetime.now()
    print("Время старта: " + str(start))
    k = 0

    task = pubPost()
    tasks.append(task)
    finish = datetime.datetime.now()

    # вычитаем время старта из времени окончания
    print("Время работы: " + str(finish - start))
    # планируем одновременные вызовы
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())
