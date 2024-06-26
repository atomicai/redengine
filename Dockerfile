FROM python:latest

RUN pip install rethinkdb
RUN pip install pyyaml
COPY init_rethinkdb.py /init_rethinkdb.py
COPY config.yaml /config.yaml


CMD ["python", "/init_rethinkdb.py"]