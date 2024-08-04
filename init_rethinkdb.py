import rethinkdb as r
import time
import yaml
from pathlib import Path
import os

with open(str(Path(os.getcwd()) / "config.yaml")) as fp:
    config = yaml.safe_load(fp)

rdb = r.RethinkDB()
conn = rdb.connect(host='localhost', port=28015)

if not rdb.db_list().contains(config["db"]["database"]).run(conn):
    rdb.db_create(config["db"]["database"]).run(conn)

if not rdb.db(config["db"]["database"]).table_list().contains('users').run(conn):
    rdb.db(config["db"]["database"]).table_create('users', primary_key='user_id').run(conn)

print("Database and table created successfully!")
conn.close()
