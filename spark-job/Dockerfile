FROM bitnami/spark:3
WORKDIR /app
COPY stream_job.py /app/
# در صورت نیاز، requirements.txt برای pyspark اضافه کنید
ENTRYPOINT [ "/opt/bitnami/spark/bin/spark-submit", "--master", "spark://spark-master:7077", "/app/stream_job.py" ]
