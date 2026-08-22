import asyncio
import json
import random
from fastapi import FastAPI
from fastapi.responses import StreamingResponse
from pyventus.events import EventLinker, AsyncIOEventEmitter
import time
from datetime import datetime
from kafka import KafkaProducer

# Kafka Configuration Done Here
KAFKA_TOPIC = "userevents"
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer = lambda v: json.dumps(v).encode('utf-8')
)

event_type = ["order_created","order_received","order_shipped","order_processing","order_cancelled","order_return"]
city_name = ["Mumbai","Delhi","Bengaluru","Hyderabad","Chennai","Kolkata","Ahmedabad","Pune","Jaipur","Surat","Lucknow","Kanpur","Nagpur","Indore","Bhopal","Patna","Vadodara","Visakhapatnam","Coimbatore","Chandigarh","Kochi","Guwahati","Varanasi"]

def generate_ecommerce_event():
    while True:
        event = {
            "event_id": f"evt_{random.randint(10000, 99999)}",
            "user_id": f"usr_{random.randint(100, 999)}",
            "product_id": f"prd_{random.randint(100, 999)}",
            "event_type": random.choice(event_type),
            "amount": round(random.uniform(10, 99999), 2),
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "city": random.choice(city_name)
        }
        yield event

for event in generate_ecommerce_event():
    producer.send(KAFKA_TOPIC, value=event)
    if event['event_type'] in ['created', 'done']:
        producer.send('order_events', value=event)
    elif event['event_type'] == 'received':
        producer.send('payment_events', value=event)
    print(f"Produced: {event}")
    time.sleep(1)