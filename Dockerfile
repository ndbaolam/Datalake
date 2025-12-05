FROM apache/spark:3.5.7-python3

# Copy JAR vào đúng thư mục
COPY jars/*.jar /opt/spark/jars/

# Auto add to Spark classpath
ENV SPARK_CLASSPATH="/opt/spark/jars/*"
