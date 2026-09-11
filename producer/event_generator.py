import asyncio
import json
import random
from fastapi import FastAPI
from fastapi.responses import StreamingResponse
from pyventus.events import EventLinker, AsyncIOEventEmitter
import time
from datetime import datetime,timedelta
from kafka import KafkaProducer

# # Kafka Configuration Done Here
# KAFKA_TOPIC = "userevents"
# producer = KafkaProducer(
#     bootstrap_servers=['localhost:9092'],
#     value_serializer = lambda v: json.dumps(v).encode('utf-8')
# )
app = FastAPI(title="E-commerce Dummy DATA API")
event_type = ["purchase","add_to_cart"]
# event_type = ["order_created","order_received","order_shipped","order_processing","order_cancelled","order_return"]
city_name = ["Mumbai","Delhi","Bengaluru","Hyderabad","Chennai","Kolkata","Ahmedabad","Pune","Jaipur","Surat","Lucknow","Kanpur","Nagpur","Indore","Bhopal","Patna","Vadodara","Visakhapatnam","Coimbatore","Chandigarh","Kochi","Guwahati","Varanasi"]

NUM_USERS = 200
user_pool = []
first_names = ["Amit","Riya","Iqbal","Luke","Hamza","John","Rahul","Anita","Suresh","Neha","Yasin"]
last_names = ["Sharma","Verma","Khan","Dares","Pathan","Chris","Kumar","Reddy","Nair","Joshi","Nagori"]

for i in range(1, NUM_USERS + 1):
    uid = f"usr_{i:03d}"
    fname = random.choice(first_names)
    lname = random.choice(last_names)
    user_pool.append({
        "user_id": uid,
        "user_name": f"{fname} {lname}",
        "email": f"{fname.lower()}.{lname.lower()}{i}@example.com",
        "signup_date": (datetime.now() - timedelta(days=random.randint(30, 900))).strftime("%Y-%m-%d"),
        "city": random.choice(city_name),
        "age": random.randint(18, 60),
        "gender": random.choice(["M", "F", "Other"]),
        "membership": random.choice(["Free", "Silver", "Gold", "Platinum"])
    })

product_pool = []
categories = ["Electronics","Fashion","Grocery","Home","Beauty","Sports","Books","Toys","Electronics"]
for i in range(1, 101):
    product_pool.append({
        "product_id": f"prd_{i:03d}",
        "product_name": f"Product {i}",
        "category": random.choice(categories),
        "price": round(random.uniform(50, 5000), 2)
    })

def generate_ecommerce_event():
    user = random.choice(user_pool)
    product = random.choice(product_pool)
    qty = random.randint(1, 5)
    event_type_choice = random.choice(event_type)

    return {
        "event_id": f"evt_{random.randint(100000, 999999)}",
        "user_id": user["user_id"],
        "user_name": user["user_name"],
        "email": user["email"],
        "membership": user["membership"],
        "gender": user["gender"],
        "age": user["age"],
        "product_id": product["product_id"],
        "product_name": product["product_name"],
        "category": product["category"],
        "unit_price": product["price"],
        "event_type": event_type_choice,
        "amount": round(product["price"] * qty, 2) if event_type_choice in ("purchase","add_to_cart") else 0,
        "qty": qty,
        "payment_method": random.choice(["UPI","Credit Card","Debit Card","Net Banking","COD"]),
        "device": random.choice(["Mobile","Desktop","Tablet"]),
        "city": user["city"],
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }
print(generate_ecommerce_event())
# def generate_ecommerce_event():
#     # while True:
#         return {
#             "event_id": f"evt_{random.randint(10000, 99999)}",
#             "user_id": f"usr_{random.randint(100, 999)}",
#             "product_id": f"prd_{random.randint(100, 999)}",
#             "event_type": random.choice(event_type),
#             "amount": round(random.uniform(10, 99999), 2),
#             "qty": random.randint(1,99),
#             "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
#             "city": random.choice(city_name)
#         }
#         # yield event



@app.get("/")
def home():
    return {
        "status" : "running",
        "message" : "E-Commerce API is running"
    }

@app.get("/event")
def get_event():
    return generate_ecommerce_event()


@app.get("/events")
def get_events(count: int = 10):
    count = min(count,100)
    return [
        generate_ecommerce_event()
        for _ in range(count)
    ]

# for event in generate_ecommerce_event():
#     producer.send(KAFKA_TOPIC, value=event)
#     if event['event_type'] in ['created', 'done']:
#         producer.send('order_events', value=event)
#     elif event['event_type'] == 'received':
#         producer.send('payment_events', value=event)
#     print(f"Produced: {event}")
#     time.sleep(1)