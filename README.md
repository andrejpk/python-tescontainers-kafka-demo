# Example PySpark/Kafka Integrartion test using TestContainers

Using [TestContainers](https://testcontainers-python.readthedocs.io/en/latest/) to create an ad-hoc Kafka instance to testing PySpark operations producing and consuming to/from Kafka.

**This gives me an error on the last Spark->Kafka prodAucer step: ***

```text
WARN NetworkClient: [Producer clientId=producer-1] Connection to node 1 (localhost/127.0.0.1:57617) could not be established. Broker may not be available.
```

I'll update the repo once this is resolved.

## Running

1. Init the virtual environment
2. Install requirements

```shell
$ pip install -r
```

3. Run

```shell
$ python test1.py
```
