import repubsub
import asyncio
import uuid

from redengine.tdk.prime import UserAction, EventLike, ActionPost


exchange = repubsub.Exchange(
    "pubsub_demo", db="meetingsBook", host="localhost", port=28015
)

id = uuid.uuid4()


async def reader():
    filter_func = lambda topic: topic.match(r"fights\.superheroes.*")

    queue = exchange.queue(filter_func)

    filter_func = lambda tags: tags.contains("fight", "superhero")

    for tags, payload in exchange.queue(filter_func).subscription():
        fighter1, fighter2 = payload["participants"]
        print(fighter1, "got in a fight with", fighter2)


async def pubPost():

    topic = exchange.topic(["superhero", "fight", "supervillain"])
    topic.publish(
        {
            "interaction_type": "tussle",
            "participants": ["Batman", "Joker"],
        }
    )

    # await asyncio.sleep(0.5)
    future = asyncio.create_task(reader())

    posts = ActionPost().get_post()

    await future


async def main():
    tasks = []
    for i in range(1, 1000001):
        task = pubPost()
        tasks.append(task)
    # планируем одновременные вызовы
    L = await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())
