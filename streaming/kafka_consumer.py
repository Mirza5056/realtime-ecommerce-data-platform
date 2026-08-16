import json
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    'user_events',
    bootstrap_servers=['localhost:9092'],
    group_id = 'fraud-detector-group',
    value_serializer = lambda x : json.loads(x.decode('utf-8'))
)

detector = RapidTransactionDetector(max_transactions=3, window_seconds=120)

FRAUD_AMOUNT = 80000.00
for message in consumer:
    event = message.value
    try:
        detector.process_transaction(event)
    except ValueError as error:
        print(f"Alert {error}")