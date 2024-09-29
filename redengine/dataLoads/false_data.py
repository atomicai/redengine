import ast
from pathlib import Path
from quart import jsonify
import rethinkdb as r
import uuid
from pathlib import Path
from typing import List, Union
import re
import json

# data_dir = Path.home() / "Projects" / "redengine" / "redengine" / "dataLoads" / "dataSets/"

# filename = Path("/oikSmall.csv")


# file = open('/home/alexey/pythonProject2/mockinghack/back/dataset/purchases.csv','r')
#
# arr = []
#
# for row in file:
#     arr.append(row)
#
#
#
# print(arr[0].split(";"))

emojis = [
    ":100:",
    ":flame:",
    ":eyes:",
    ":shit:",
    ":clown:",
    ":ok_hand:",
    ":like:",
    ":dislike:",
    ":smiley:",
    ":fire:",
]

rdb = r.RethinkDB()
conn = rdb.connect(host="localhost", port=28015)


# data = rdb.db('meetingsDb').table('posts').filter(        rdb.or_(
#             rdb.row['translation'].eq(None),
#             rdb.row['translation'].match(r"(\b\w+)\1"),  # Повторяющиеся слова
#             rdb.row['translation'].match(r'[\u0400-\u04FF]+')  # Русские буквы
#         )).count().run(conn)


# # Получение всех записей, где translation не пустое
# cursor = await r.table('posts').filter(
#     r.or_(
#         r.row['translation'].eq(None),  # Поле translation = null
#         r.row['translation'].match(r"[\u0400-\u04FF]+")  # Русские буквы
#     )
# ).run(conn)

posts = rdb.db("meetingsDb").table("posts").run(conn)


# for post in posts :
#     if post['translation']:
#         print(has_repeated_words(post['translation']))
result1 = []


def contains_russian(text):
    if isinstance(text, str):
        # Проверка наличия хотя бы одного русского символа
        return any("\u0400" <= char <= "\u04FF" for char in text)
    return False


def save_to_json(data, filename="data_for_fix.json"):
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)


# filtered_posts = [post for post in posts if  "translation" in post and post['translation'] and has_repeated_words(post['translation'])]
for post in posts:
    if "translation" in post and post["translation"]:

        if contains_russian(post["translation"]):
            result1.append(post)


nullable_posts = (
    rdb.db("meetingsDb")
    .table("posts")
    .filter(
        rdb.or_(
            rdb.row["translation"].eq(None),
        )
    )
    .run(conn)
)

res = []

for pos in nullable_posts:
    res.append(pos)

result = {"posts_with_russian": result1, "posts_with_none": res}

# Сохранение объекта в JSON файл
save_to_json(result)


# Запрос для поиска записей, соответствующих условиям
