from testcontainers.kafka import KafkaContainer

from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

from pyspark.sql import SparkSession
from pyspark.sql.functions import to_json, struct
from datetime import datetime, date

print("--- Creating the Kafka TestContainer and wait for it to start ---")

with KafkaContainer() as kafka:
   kafkaConnectionString = kafka \
      .get_bootstrap_server()

   print('Kafka connection string', kafkaConnectionString)


   print('--- Creating the Kafka topic ---')
   kafkaTopic = "testTopic"

   admin_client = KafkaAdminClient(
    bootstrap_servers=kafkaConnectionString,
    client_id='test_client'
   )

   topic_list = []
   topic_list.append(NewTopic(name=kafkaTopic, num_partitions=1, replication_factor=1))

   adminResp = admin_client.create_topics(new_topics=topic_list, validate_only=False)
   print(f"Topic '{kafkaTopic}' created successfully.")
   print('response', adminResp)


   print("--- Starting Spark and creating test data ---")

   spark = SparkSession.builder \
      .appName("KafkaTest") \
      .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2") \
      .getOrCreate()

   from pyspark.sql import Row

   df = spark.createDataFrame([
      Row(a=1, b=2., c='string1', d=date(2000, 1, 1), e=datetime(2000, 1, 1, 12, 0)),
      Row(a=2, b=3., c='string2', d=date(2000, 2, 1), e=datetime(2000, 1, 2, 12, 0)),
      Row(a=4, b=5., c='string3', d=date(2000, 3, 1), e=datetime(2000, 1, 3, 12, 0))
   ])

   df.show()
   df.printSchema()

   print("--- Publishing test data to Kafka ---")
   # writing as a batch (not streaming here)
   ds = (df
   .select(to_json(struct([df[x] for x in df.columns])).alias("value"))
   .write
   .format("kafka") 
   .option("kafka.bootstrap.servers", kafkaConnectionString) 
   .option("topic", kafkaTopic)
   .save()
   )
