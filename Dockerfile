FROM apache/spark:3.5.7-python3

COPY jars/*.jar /opt/spark/jars/
COPY conf /opt/spark/conf

COPY app /app

# Auto add to Spark classpath
ENV SPARK_CLASSPATH="/opt/spark/jars/*"

CMD ["/opt/spark/bin/spark-submit", "/app/consumer.py"]