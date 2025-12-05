from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

catalog_name = "demo"
table_name = "persons"
db_name="db"
warehouse_path = "./warehouse"

spark = (
    SparkSession.builder
    .appName("iceberg-demo")
    # ENABLE Iceberg
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.demo", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.demo.type", "hadoop")
    .config("spark.sql.catalog.demo.warehouse", warehouse_path)
    .config("spark.sql.defaultCatalog", catalog_name)
    .getOrCreate()
)


sc = spark.sparkContext

sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", "console")
sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "console123")
sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "http://minio:9000")
sc._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
sc._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "false")
sc._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
sc._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "false")
#Iceberg

schema = StructType([
    StructField('name', StringType(), True),
    StructField('age', IntegerType(), True),
    StructField('job_title', StringType(), True)
])
data = [("person1", 28, "Doctor"), ("person2", 35, "Singer"), ("person3", 42, "Teacher")]
df = spark.createDataFrame(data, schema=schema)

df.show()

# df.write.csv("s3a://bronze/test_output.csv", header=True, mode="overwrite")
spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog_name}.{db_name}")

spark.sql("SHOW CATALOGS").show()

df.write.format("iceberg").mode("overwrite").saveAsTable(f"{table_name}")
iceberg_df = spark.read.format("iceberg").load(f"{table_name}")
iceberg_df.printSchema()
iceberg_df.show()

df.write.csv("s3a://silver/person.csv", header=True, mode="overwrite")

spark.stop()