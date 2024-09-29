import ast
from pathlib import Path

import rethinkdb as r
import uuid
import ast
from pathlib import Path
from typing import List, Union
import re
import json
from redengine.configuring import Config
from datetime import datetime

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
conn = rdb.connect(host=Config.app.host, port=28015)

now = datetime.now()
year_month = now.strftime("%Y_%m")
table_name = f"events_{year_month}"
if not rdb.db_list().contains("meetingsDb").run(conn):
    rdb.db_create("meetingsDb").run(conn)

if not rdb.db("meetingsDb").table_list().contains(table_name).run(conn):
    rdb.db("meetingsDb").table_create(table_name).run(conn)


# data_dir = Path.home() / "projects" / "redengine"/"redengine"/"dataLoads"/"dataSets/"
# filename = Path("polaroids.ai.data_test.json")
# data_dir = Path(data_dir)
# with open(data_dir / filename) as json_file:
#     data = json.load(json_file)
#     for p in data["posts"]:
#         if "title" not in p:
#             p["title"]=None
#         repeat = rdb.db('meetingsDb').table('movies').filter({'title':p["title"]}).nth(0).default(None).run(conn)
#         title_table = re.sub(r'[^a-zA-Z0-9_]', '_', p["title"])
#         title_table = ((re.sub(r'_+', '_', title_table)).strip('_')).lower()

#         if not rdb.db('meetingsDb').table_list().contains(f'events_{title_table}').run(conn):
#             rdb.db('meetingsDb').table_create(f'events_{title_table}').run(conn)
#         if not repeat:

#             id = uuid.uuid4()
#             if "type" in p and p["type"]=='movie':
#                 rdb.db('meetingsDb').table('movies').insert({'title': p["title"]}).run(conn)
