import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, window, when, concat_ws,
    sum as _sum, to_json, struct, current_timestamp, expr
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType
)

#konfigurasi kafka yang sama dengan compose
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
INPUT_TOPIC = "transactions"
VALID_OUTPUT_TOPIC = "transactions_valid"
DLQ_TOPIC = "transactions_dlq"

def get_spark_session():
    return SparkSession.builder \
        .appName("RealTimeProcessing") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()

def get_schema():
    return StructType([
        StructField("user_id", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("timestamp", StringType(), True),
        StructField("source", StringType(), True),
    ])

def row_validation(parsed_stream):

    err_user_id = when(col("user_id").isNull(),  "MISSING USER_ID")

    err_amount = when(col("amount").isNull(), "MISSING AMOUNT") \
                 .when((col("amount") < 1) | (col("amount") >  10000000), "OUT OF RANGE")

    err_timestamp = when(col("timestamp").isNull(), "MISSING TIMESTAMP")

    err_source = when(col("source").isNull() | (~col("source").isin(["mobile","web","pos"])), "INVALID SOURCE")

    err_late = when(col("timestamp") < (current_timestamp() - expr("INTERVAL 3 MINUTES")), "LATE_EVENT_EXCEEDS_WATERMARK")

    stream_erros_data = parsed_stream.withColumn(
        "error_reason",
        concat_ws("; ", err_user_id, err_amount, err_timestamp, err_late, err_source)
    )

    validated_data = stream_erros_data.withColumn(
        "is_valid",
        when(col("error_reason") == "", True).otherwise(False)
    ).withColumn(
        "error_reason",
        when(col("is_valid"), None).otherwise(col("error_reason"))
    )

    return validated_data

#variable menyimpan cumulative di console
cumulative_running_total = 0.0

def main():
    spark = get_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print("Membaca data dari kafka")

    #spark membaca kafka
    raw_stream = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", INPUT_TOPIC) \
        .option("startingOffsets", "latest") \
        .load()

    schema = get_schema()

    #ubah data kafka yang bianry menjadi string, kemudian parse menjadi JSON
    parsed_stream = raw_stream.selectExpr("CAST(value AS STRING) as json_val") \
        .select(from_json(col("json_val"), schema).alias("data")) \
        .select("data.*") \
        .withColumn("timestamp", to_timestamp(col("timestamp")))

    #kita panggil function validasi
    df_transaction = row_validation(parsed_stream=parsed_stream)

    validated_stream_data = df_transaction \
        .filter(col("is_valid") ==  True) \
        .withWatermark("timestamp", "3 minutes") \
        .dropDuplicates(['user_id','timestamp'])

    dlq_stream_data = df_transaction.filter(col("is_valid") == False)

    windowed_stream_data = validated_stream_data \
        .groupBy(
            window(col("timestamp"), "1 minute")
        ) \
        .count() \
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("count").alias("total_transactions")
        )

    def foreach_batch_sink(batch_df, batch_id):
        global cumulative_running_total

        if batch_df.isEmpty():
            return

        #jika valid_batch ada datanya
        batch_df.select(
            col("user_id").cast("string").alias("key"),
            to_json(struct("*")).alias("value")
        ).write \
         .format("kafka") \
         .option("kafka.bootstrap.servers", KAFKA_BROKER) \
         .option("topic", VALID_OUTPUT_TOPIC) \
         .save()

        # Hitung akumulasi nominal transaksi (sum)
        batch_total = batch_df.select(_sum("amount")).collect()[0][0] or 0.0
        cumulative_running_total += batch_total

        #buat output di console
        spark_output_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print("="*50)
        print(f"BATCH ID        = {batch_id}")
        print(f"timestamp       = {spark_output_time}")
        print(f"running_total   = {cumulative_running_total}")

    def not_valid_batch_sink(batch_df,batch_id):

        if batch_df.isEmpty():
            return

        batch_df.select(
            col("user_id").cast("string").alias("key"),
            to_json(struct("*")).alias("value")
        ).write \
         .format("kafka") \
         .option("kafka.bootstrap.servers", KAFKA_BROKER) \
         .option("topic", DLQ_TOPIC) \
         .save()
           
        print(f"DLQ batch {batch_id} : {batch_df.count()} => terkirim ke {DLQ_TOPIC}")    

    #melakukan foreachBatch untuk menjalankan streaming
    query_valid = validated_stream_data.writeStream \
        .foreachBatch(foreach_batch_sink) \
        .start()

    query_dlq = dlq_stream_data.writeStream \
        .foreachBatch(not_valid_batch_sink) \
        .start()

    query_window = windowed_stream_data.writeStream \
        .format("console") \
        .outputMode("update") \
        .option("truncate", False) \
        .option("checkpointLocation", "./checkpoints/window") \
        .start()

    try:
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        print("\nStreaming job dihentikan oleh user.")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()

    