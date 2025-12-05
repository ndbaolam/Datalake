from pyspark.sql import SparkSession

spark_version = '3.5.7'

packages = [
    'org.apache.hadoop.fs.s3a.S3AFileSystem',
]

spark = (
    SparkSession.builder.appName("TestApp")
    # .config("spark.jars.packages", ",".join(packages))               
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000/")
    .config("spark.hadoop.fs.s3a.access.key", "console")
    .config("spark.hadoop.fs.s3a.secret.key", "console123")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")    
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate()
)

df = spark.createDataFrame(
    [(1, "Alice"), (2, "Bob"), (3, "Cathy")],
    ["id", "name"]
)

df.show()

df.write.csv("s3a://bronze/test_output.csv", header=True, mode="overwrite")

spark.stop()