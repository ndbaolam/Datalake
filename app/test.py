from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", "console")
sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "console123")
sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "http://minio:9000")
sc._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
sc._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "false")
sc._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
sc._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "false")

df = spark.createDataFrame(
    [(1, "Alice"), (2, "Bob"), (3, "Cathy")],
    ["id", "name"]
)

df.show()

df.write.csv("s3a://bronze/test_output.csv", header=True, mode="overwrite")

spark.stop()