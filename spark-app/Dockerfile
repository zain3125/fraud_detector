FROM apache/spark:3.5.6

USER root
WORKDIR /app

COPY fraud_streaming.py .
COPY requirements.txt .

RUN pip install -r requirements.txt

CMD ["spark-submit", "--master", "spark://spark-master:7077", "fraud_streaming.py"]
