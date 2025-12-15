from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("SparkKafka")
    .getOrCreate()
)

df = (
    spark.readStream
    .format("kafka")
    .option(
        "kafka.bootstrap.servers",
        "broker1:9092,broker2:9092,broker3:9092"
    )
    .option("subscribe", "my_topic")
    .option("startingOffsets", "earliest")
    .load()
)

df_value = df.selectExpr(
    "CAST(key AS STRING)",
    "CAST(value AS STRING)",
    "topic",
    "partition",
    "offset",
    "timestamp"
)

query = (
    df_value.writeStream
    .format("console")
    .option("truncate", False)
    .start()
)

query.awaitTermination()
