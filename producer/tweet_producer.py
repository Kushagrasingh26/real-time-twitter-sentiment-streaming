import time
import random
from kafka import KafkaProducer
import json

tweets = [
    "I love this product!",
    "Worst service ever.",
    "Amazing experience!",
    "Not happy with the quality.",
    "Super fast delivery!",
    "Terrible support team."
]

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

topic = "twitter_stream"

while True:
    tweet = random.choice(tweets)
    producer.send(topic, {"tweet": tweet})
    print(f"Sent: {tweet}")
    time.sleep(1)
