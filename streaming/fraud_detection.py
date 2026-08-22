import json
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    'userevents',
    bootstrap_servers=['localhost:9092'],
    group_id = 'fraud-detector-group',
    value_deserializer = lambda x : json.loads(x.decode('utf-8'))
)

FRAUD_AMOUNT = 80000.00
for message in consumer:
    event = message.value
    if event['amount'] > FRAUD_AMOUNT:
        print(f"FRAUD ALERT: High transaction amount ${event['amount']} detected!")
        print(f"Event Details: {event}")