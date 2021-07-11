from json import loads

from kafka import KafkaConsumer

consumer = KafkaConsumer(
    'topico-csv',
     bootstrap_servers=['localhost:9092'],
     auto_offset_reset='earliest',
     enable_auto_commit=True,
     group_id='my-group2',
     value_deserializer=lambda x: loads(x.decode('utf-8')))

for message in consumer:
    print(message.value)