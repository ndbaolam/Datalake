from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import os

catalog = os.getenv("ICEBERG_CATALOG", "demo")
db = os.getenv("ICEBERG_DB", "db")
table = os.getenv("ICEBERG_TABLE", "persons")

spark = (
    SparkSession.builder
    .appName("iceberg-demo")
    .getOrCreate()
)

schema = StructType([
    StructField("name", StringType()),
    StructField("age", IntegerType()),
    StructField("job_title", StringType())
])

data = [
    ("person1", 28, "Doctor"),
    ("person2", 35, "Singer"),
    ("person3", 42, "Teacher")
]

df = spark.createDataFrame(data, schema)

spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog}.{db}")

df.write.format("iceberg").mode("overwrite").saveAsTable(f"{db}.{table}")

spark.read.format("iceberg").load(f"{db}.{table}").show()

df.write.csv("s3a://silver/person.csv", header=True, mode="overwrite")

spark.stop()
