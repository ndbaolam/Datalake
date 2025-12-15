from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

spark = SparkSession.builder.appName("SparkKafkaProducer").getOrCreate()

data = [
    ("user-1", '{"user":"alice","action":"login"}'),
    ("user-2", '{"user":"bob","action":"click"}'),
]

df = spark.createDataFrame(data, ["key", "value"])

(
    df.selectExpr(
        "CAST(key AS STRING)",
        "CAST(value AS STRING)"
    )
    .write
    .format("kafka")
    .option(
        "kafka.bootstrap.servers",
        "broker1:9092,broker2:9092,broker3:9092"
    )
    .option("topic", "my_topic")
    .save()
)
