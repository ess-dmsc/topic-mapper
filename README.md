# Topic Mapper

Map Kafka topics many-to-one.
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
