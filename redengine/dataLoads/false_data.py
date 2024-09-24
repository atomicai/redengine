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

emojis = [":100:",":flame:",":eyes:",":shit:",":clown:",":ok_hand:",":like:",":dislike:",":smiley:",":fire:"]

rdb = r.RethinkDB()
conn = rdb.connect(host='localhost', port=28015)




data = rdb.db('meetingsDb').table('posts').filter({"translation":None}).count().run(conn)
print('====',data)

