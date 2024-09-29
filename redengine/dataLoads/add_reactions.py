import ast
from pathlib import Path

import rethinkdb as r
import uuid
from pathlib import Path
from typing import List, Union

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


if not rdb.db_list().contains("meetingsDb").run(conn):
    rdb.db_create("meetingsDb").run(conn)

if not rdb.db("meetingsDb").table_list().contains("emojis").run(conn):
    rdb.db("meetingsDb").table_create("emojis").run(conn)


data_dir = (
    Path.home() / "projects" / "redengine" / "redengine" / "dataLoads" / "dataSets/"
)
filename = Path("emojis.json")
data_dir = Path(data_dir)
with open(data_dir / filename) as json_file:
    data = json.load(json_file)
    for p in data:
        rdb.db("meetingsDb").table("emojis").insert(
            {"label": p, "value": data[p], "active": False}
        ).run(conn)
    rdb.db("meetingsDb").table("emojis").filter(
        lambda emoji: rdb.expr(emojis).contains(emoji["label"])
    ).update({"active": True}).run(conn)
