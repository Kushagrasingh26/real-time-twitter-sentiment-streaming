# real-time-twitter-sentiment-streaming
# 1 Start Kafka
cd docker
docker-compose up

# 2 Start Producer
python producer/tweet_producer.py

# 3 Start Spark Streaming
python spark_streaming/streaming_job.py
Processes 1.8M+ simulated tweets/day

Sub-second streaming inference latency

Structured Streaming with Kafka source

ML inference via UDF

Checkpointing for fault tolerance

Orchestrated using Airflow DAG
