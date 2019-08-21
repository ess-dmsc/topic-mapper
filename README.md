# Topic Mapper

Map Kafka topics many-to-one. Messages from the input topics are forwarded into the output topic. Kafka message timestamps are preserved. Messages can also optionally be filtered such that only messages of a particular flatbuffer schema ID get forwarded.

This is intended as a development tool not something to use in production. Think carefully before using this, particularly with high-volume topics, as the data in the input topics are replicated.

## To run
Requires Python 3.6+. Install dependencies with pip:
```
pip install -r requirements.txt
```

Mapping is currently hardcoded in `topic_mapper.py`.

Run `topic_mapper.py`:
```
python topic_mapper.py --broker localhost
```

## To make distributable
A standalone distributable package can be made using cx_freeze:
```
cxfreeze topic_mapper.py --target-dir dist
```
