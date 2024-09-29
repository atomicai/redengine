import ast
from pathlib import Path

import rethinkdb as r
import uuid
import ast

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

rdb = r.RethinkDB()
conn = rdb.connect(host="localhost", port=28015)


if not rdb.db_list().contains("meetingsDb").run(conn):
    rdb.db_create("meetingsDb").run(conn)

if not rdb.db("meetingsDb").table_list().contains("movies").run(conn):
    rdb.db("meetingsDb").table_create("movies").run(conn)


data_dir = (
    Path.home() / "projects" / "redengine" / "redengine" / "dataLoads" / "dataSets/"
)
filename = Path("polaroids.ai.data_fresh.json")
data_dir = Path(data_dir)
with open(data_dir / filename) as json_file:
    data = json.load(json_file)
    for p in data["posts"]:
        if "title" not in p:
            p["title"] = None
        repeat = (
            rdb.db("meetingsDb")
            .table("movies")
            .filter({"title": p["title"]})
            .nth(0)
            .default(None)
            .run(conn)
        )

        if not repeat:

            id = uuid.uuid4()
            if "type" in p and p["type"] == "movie":
                rdb.db("meetingsDb").table("movies").insert({"title": p["title"]}).run(
                    conn
                )
