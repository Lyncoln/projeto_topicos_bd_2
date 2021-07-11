import csv
import json
import random
from time import sleep, time
from json import dumps
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x:
                         json.dumps(x).encode('utf-8'))

print("Ctrl+c to Stop")
while True:
    with open('Date=2004-10-01.csv', 'r') as file:
        reader = csv.reader(file, delimiter=',')
        for row in reader:
            producer.send('topico-csv', row)
            producer.flush()
            sleep(5)
