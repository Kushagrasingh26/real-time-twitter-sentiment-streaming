from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StringType
from sentiment_model import analyze_sentiment
from pyspark.sql.functions import udf

spark = SparkSession.builder \
    .appName("TwitterSentimentStreaming") \
    .getOrCreate()

schema = StructType().add("tweet", StringType())

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "twitter_stream") \
    .load()

json_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

sentiment_udf = udf(analyze_sentiment, StringType())

final_df = json_df.withColumn("sentiment", sentiment_udf(col("tweet")))

query = final_df.writeStream \
    .format("parquet") \
    .option("path", "outputs/sentiment_results") \
    .option("checkpointLocation", "outputs/checkpoints") \
    .start()

query.awaitTermination()
