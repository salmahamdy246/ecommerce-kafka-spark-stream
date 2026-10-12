from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, round as spark_round
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType

def main():
    spark = SparkSession.builder \
        .appName("ShopPulse-SparkStreamingIngestion") \
        .config("spark.sql.parquet.writeLegacyFormat", "true") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    schema = StructType([
        StructField("order_id", IntegerType(), True),
        StructField("customer_id", IntegerType(), True),
        StructField("product_id", IntegerType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("price", DoubleType(), True),
        StructField("order_time", StringType(), True)
    ])

    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "topic1_logs") \
        .option("startingOffsets", "earliest") \
        .load()

    parsed_df = kafka_df \
        .selectExpr("CAST(value AS STRING) as json_payload") \
        .select(from_json(col("json_payload"), schema).alias("data")) \
        .select("data.*")

    enriched_df = parsed_df \
        .withColumn("total_amount", spark_round(col("quantity") * col("price"), 2)) \
        .withColumn("order_timestamp", to_timestamp(col("order_time"), "yyyy-MM-dd HH:mm:ss")) \
        .drop("order_time")

    hdfs_storage_path = "/user/hive/warehouse/ecommerce_dw.db/streaming_orders"
    checkpoint_location = "/user/spark/checkpoints/streaming_orders"

    query = enriched_df.writeStream \
        .format("parquet") \
        .outputMode("append") \
        .option("path", hdfs_storage_path) \
        .option("checkpointLocation", checkpoint_location) \
        .trigger(processingTime="10 seconds") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()
