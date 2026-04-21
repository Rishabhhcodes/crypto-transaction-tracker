# import time

# print("⏳ Waiting for Kafka to be ready...")
# time.sleep(20)

# from pyspark.sql import SparkSession
# from pyspark.sql.functions import *
# from pyspark.sql.types import *

# spark = SparkSession.builder \
#     .appName("CryptoTransactions") \
#     .getOrCreate()

# df = spark.readStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", "kafka:9092") \
#     .option("subscribe", "crypto-topic") \
#     .option("startingOffsets", "latest") \
#     .load()

# json_df = df.selectExpr("CAST(value AS STRING)")

# schema = StructType([
#     StructField("tx_hash", StringType()),
#     StructField("from_address", StringType()),
#     StructField("to_address", StringType()),
#     StructField("value", DoubleType()),
#     StructField("gas", IntegerType()),
#     StructField("timestamp", LongType()),
#     StructField("is_token_tx", BooleanType())  # ✅ ADD THIS
# ])

# parsed = json_df.select(from_json(col("value"), schema).alias("data")).select("data.*")

# def write_to_postgres(batch_df, batch_id):
#     print(f"🔥 Writing batch {batch_id}, count: {batch_df.count()}")

#     batch_df.write \
#         .format("jdbc") \
#         .option("url", "jdbc:postgresql://postgres:5432/crypto") \
#         .option("dbtable", "transactions") \
#         .option("user", "postgres") \
#         .option("password", "postgres") \
#         .option("driver", "org.postgresql.Driver") \
#         .mode("append") \
#         .save()

# while True:
#     try:
#         query = parsed.writeStream \
#             .foreachBatch(write_to_postgres) \
#             .outputMode("append") \
#             .start()

#         query.awaitTermination()

#     except Exception as e:
#         print("❌ Spark error:", str(e))
#         print("🔁 Restarting in 5 seconds...")
#         time.sleep(5)

import time

print("⏳ Waiting for Kafka...")
time.sleep(20)

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("CryptoTransactions") \
    .getOrCreate()

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "crypto-topic") \
    .option("startingOffsets", "latest") \
    .load()

json_df = df.selectExpr("CAST(value AS STRING)")

schema = StructType([
    StructField("tx_hash", StringType()),
    StructField("from_address", StringType()),
    StructField("to_address", StringType()),
    StructField("value", DoubleType()),
    StructField("gas", IntegerType()),
    StructField("timestamp", LongType()),
    StructField("is_token_tx", BooleanType())
])

parsed = json_df.select(from_json(col("value"), schema).alias("data")).select("data.*")

def write_to_postgres(batch_df, batch_id):
    print(f"🔥 Writing batch {batch_id}, count: {batch_df.count()}")

    batch_df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres:5432/crypto") \
        .option("dbtable", "transactions") \
        .option("user", "postgres") \
        .option("password", "postgres") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()

query = parsed.writeStream \
    .foreachBatch(write_to_postgres) \
    .outputMode("append") \
    .start()

query.awaitTermination()