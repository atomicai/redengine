from pathlib import Path
from datetime import datetime
import abc
from redengine.etc.pattern import singleton
import rethinkdb as r
import uuid
import os
import yaml
from dotmap import DotMap
from redengine.configuring import Config
from redengine.tdk.lib import transliterate

rdb = r.RethinkDB()
rdb.set_loop_type("asyncio")


class IDBDocStore(abc.ABC):
    @abc.abstractmethod
    async def add_event(self, query: str, response):
        pass


@singleton
class IReDocStore(IDBDocStore):
    def __init__(self, host=Config.db.host, port=Config.db.port, db=Config.db.database):
        self.host = host
        self.port = port
        self.db = db
        self.connection = None

    async def connect(self):
        if not self.connection:
            self.connection = await rdb.connect(host=self.host, port=self.port)

    async def close(self):
        if self.connection:
            await self.connection.close()
            self.connection = None

    async def add_event(self, query, response):
        await (
            rdb.db(self.db)
            .table("events")
            .insert({"query": query, "response": response})
            .run(self.connection)
        )

    async def personByEmail(self, email):
        return (
            await rdb.db(self.db)
            .table("users")
            .filter({"email": email})
            .nth(0)
            .default(None)
            .run(self.connection)
        )

    async def addUserByEmail(self, email, hashed_password, refresh_token):
        await (
            rdb.db(self.db)
            .table("users")
            .insert(
                {
                    "email": email,
                    "password": hashed_password,
                    "login": email,
                    "refresh_token": refresh_token,
                    "active": True,
                    "created_at": rdb.now(),
                }
            )
            .run(self.connection)
        )

    async def addUser(self, user_id, login, hashed_password, refresh_token):
        return (
            await rdb.db(self.db)
            .table("users")
            .insert(
                {
                    "user_id": user_id,
                    "login": login,
                    "password": hashed_password,
                    "refresh_token": refresh_token,
                    "active": True,
                    "created_at": rdb.now(),
                }
            )
            .run(self.connection)
        )

    async def updateRefreshToken(self, user_id, refresh_token):
        return (
            await rdb.db(self.db)
            .table("users")
            .get({user_id})
            .update({"refresh_token": refresh_token})
            .run(self.connection)
        )

    async def deleteAccount(self, id):
        return (
            await rdb.db(self.db)
            .table("users")
            .filter({"id": id})
            .update({"active": False})
            .run(self.connection)
        )

    async def usersList(self):
        return await rdb.db(self.db).table("users").run(self.connection)

    async def personByTgUserId(self, tg_user_id):
        return (
            await rdb.db(self.db)
            .table("users")
            .filter({"tg_user_id": tg_user_id})
            .nth(0)
            .default(None)
            .run(self.connection)
        )

    async def removeFavorite(self, user_id, post_id):
        result = (
            await rdb.db(Config.db.database)
            .table("favorites_posts")
            .filter((rdb.row["user_id"] == user_id) & (rdb.row["post_id"] == post_id))
            .delete()
            .run(self.connection)
        )

        return result

    async def removeReaction(self, user_id, post_id, reaction_type):
        print(user_id, reaction_type, post_id)
        result = (
            await rdb.db(Config.db.database)
            .table("post_reaction")
            .filter(
                (rdb.row["user_id"] == user_id)
                & (rdb.row["post_id"] == post_id)
                & (rdb.row["reaction_type"] == reaction_type)
            )
            .delete()
            .run(self.connection)
        )

        return result

    async def telegramRegistration(self, tg_user_id):
        await (
            rdb.db(self.db)
            .table("users")
            .insert({"tg_user_id": tg_user_id, "active": True, "created_at": rdb.now()})
            .run(self.connection)
        )

    async def telegramUserAddPhoto(self, user_photo):
        await (
            rdb.db(self.db)
            .table("users")
            .filter({"tg_user_id": user_photo["tg_user_id"]})
            .update(user_photo)
            .run(self.connection)
        )

    async def userAddPhoto(self, user_id, file_path):
        await (
            rdb.db(self.db)
            .table("users")
            .filter({"user_id": user_id})
            .update({"file_path": file_path})
            .run(self.connection)
        )

    async def telegramUserAddInfo(self, user_info):
        await (
            rdb.db(self.db)
            .table("users")
            .filter({"tg_user_id": user_info["tg_user_id"]})
            .update(user_info)
            .run(self.connection)
        )

    async def userAddInfo(self, user_id, user_info):
        await (
            rdb.db(self.db)
            .table("users")
            .filter({"user_id": user_id})
            .update(user_info)
            .run(self.connection)
        )

    async def sendReaction(self, user_id, user_reaction):
        user_reaction["user_id"] = user_id
        user_reaction["created_at"] = rdb.now()

        # Проверяем, существует ли уже реакция от этого пользователя на этот пост
        existing_reaction = (
            await rdb.db(self.db)
            .table("post_reaction")
            .filter(
                (rdb.row["post_id"] == user_reaction["post_id"])
                & (rdb.row["user_id"] == user_id)
            )
            .nth(0)
            .default(None)
            .run(self.connection)
        )

        # Если существует, обновляем ее, если нет, создаем новую
        if existing_reaction:
            await (
                rdb.db(self.db)
                .table("post_reaction")
                .filter(
                    (rdb.row["post_id"] == user_reaction["post_id"])
                    & (rdb.row["user_id"] == user_id)
                )
                .update({"reaction_type": user_reaction["reaction_type"]})
                .run(self.connection)
            )
        else:
            await (
                rdb.db(self.db)
                .table("post_reaction")
                .insert(user_reaction)
                .run(self.connection)
            )

    async def sendReactionByUser(self, user_id, user_reaction):
        now = datetime.now()
        user_reaction["user_id"] = user_id
        user_reaction["created_at"] = rdb.now()
        year_month = now.strftime("%Y_%m")
        table_name = f"events_{year_month}"
        await (
            rdb.db(self.db).table(table_name).insert(user_reaction).run(self.connection)
        )

    async def countPostsOfTime(self, user_id, start_time, end_time):
        now = datetime.now()
        year_month = now.strftime("%Y_%m")
        table_name = f"events_{year_month}"

        return (
            await rdb.db(Config.db.database)
            .table(table_name)
            .filter(
                (rdb.row["user_id"] == user_id)
                & (rdb.row["created_at"] >= start_time)
                & (rdb.row["created_at"] <= end_time)
            )
            .count()
            .run(self.connection)
        )

    async def cursorChat(self, user_id, chat_user_id):
        return (
            await rdb.db(Config.db.database)
            .table("messages")
            .filter(
                (rdb.row["sender_id"] == user_id)
                & (rdb.row["receiver_id"] == chat_user_id)
                | (rdb.row["sender_id"] == chat_user_id)
                & (rdb.row["receiver_id"] == user_id)
            )
            .changes()
            .run(self.connection)
        )

    async def addMessage(self, user_id, chat_user_id, message):
        return (
            await rdb.db(Config.db.database)
            .table("messages")
            .insert(
                {
                    "sender_id": user_id,
                    "receiver_id": chat_user_id,
                    "message": message,
                    "created_at": rdb.now(),
                }
            )
            .run(self.connection)
        )

    async def personById(self, user_id):
        return (
            await rdb.db(Config.db.database)
            .table("users")
            .get(user_id)
            .run(self.connection)
        )

    async def userEvents(self, user_id):
        now = datetime.now()
        year_month = now.strftime("%Y_%m")
        events_table_name = f"events_{year_month}"
        return (
            await rdb.db(Config.db.database)
            .table(events_table_name)
            .filter({"user_id": user_id})
            .pluck("post_id")
            .run(self.connection)
        )

    async def keyword(self, user_id, keyword_id):
        now = datetime.now()
        year_month = now.strftime("%Y_%m")
        events_table_name = f"events_{year_month}"
        return (
            await rdb.db(Config.db.database)
            .table(events_table_name)
            .filter({"user_id": user_id})
            .pluck("post_id")
            .run(self.connection)
        )

    async def keywordsByPostIds(self, post_ids):
        return (
            await rdb.db(Config.db.database)
            .table("posts")
            .get_all(*post_ids)
            .pluck("keywords")
            .run(self.connection)
        )

    async def personByLogin(self, nickname):
        return (
            await rdb.db(Config.db.database)
            .table("users")
            .filter({"login": nickname})
            .nth(0)
            .default(None)
            .run(self.connection)
        )

    async def addFavorites(self, user_id, post_id):
        return (
            await rdb.db(Config.db.database)
            .table("favorites_posts")
            .insert({"user_id": user_id, "post_id": post_id})
            .run(self.connection)
        )

    async def addSearchKeyphrases(self, user_id, data):
        return (
            await rdb.db(Config.db.database)
            .table("search_keyphrases")
            .insert(
                {
                    "user_id": user_id,
                    "title": data["messages"][0]["title"],
                    "content": data["messages"][0]["content"],
                    "max-tokens": data["max_tokens"],
                    "temperature": data["temperature"],
                    "response": data["answer"],
                    "created_at": rdb.now(),
                }
            )
            .run(self.connection)
        )

    async def showFavorites(self, user_id):
        return (
            await rdb.db(Config.db.database)
            .table("favorites_posts")
            .filter({"user_id": user_id})
            .map(lambda doc: doc["post_id"])
            .run(self.connection)
        )

    async def PostById(self, post_ids):
        return (
            await rdb.db(Config.db.database)
            .table("posts")
            .filter(lambda post: rdb.expr(post_ids).contains(post["id"]))
            .eq_join("book_id", rdb.db(Config.db.database).table("books"))
            .zip()
            .eq_join("author_id", rdb.db(Config.db.database).table("authors"))
            .zip()
            .run(self.connection)
        )

    async def messages(self, user_id, chat_user_id, page, limit):
        return (
            await rdb.db(Config.db.database)
            .table("messages")
            .filter(
                (r.row["sender_id"] == user_id) & (r.row["receiver_id"] == chat_user_id)
                | (r.row["sender_id"] == chat_user_id)
                & (r.row["receiver_id"] == user_id)
            )
            .order_by(r.desc("created_at"))
            .slice((page - 1) * limit, page * limit)
            .run(self.connection)
        )


# RethinkDb = IReDocStore()


__all__ = ["IReDocStore"]
