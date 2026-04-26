FROM apache/spark:3.5.6

WORKDIR /app

COPY spark-app/requirements.txt .
USER root
RUN pip install --no-cache-dir -r requirements.txt

COPY utils/ ./utils/

COPY spark-app/ .

USER spark

CMD ["/opt/spark/bin/spark-submit", "--master", "spark://spark-master:7077", "fraud_streaming.py"]
