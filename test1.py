from testcontainers.kafka import KafkaContainer

with KafkaContainer() as kafka:
   connection = kafka.get_bootstrap_server()

print('Kafka connection string', connection)