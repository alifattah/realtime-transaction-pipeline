FROM bitnami/spark:3

# Set working directory
WORKDIR /app

# Set HOME environment variable
ENV HOME=/tmp

# Copy your Spark job script
COPY stream_job.py /app/

# Create Ivy cache directory for dependency management
RUN mkdir -p /app/.ivy2

# Set IVY_HOME environment variable
ENV IVY_HOME=/app/.ivy2

# Switch to root user for running the Spark job
USER root

# Define the entrypoint to run the Spark job with --packages for the Kafka connector
ENTRYPOINT [ "/opt/bitnami/spark/bin/spark-submit", "--master", "spark://spark_master:7077", "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2", "--conf", "spark.driver.extraJavaOptions=-Divy.home=/app/.ivy2", "--conf", "spark.executor.extraJavaOptions=-Divy.home=/app/.ivy2", "/app/stream_job.py" ]
