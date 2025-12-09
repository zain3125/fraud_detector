FROM python:3.12-slim

WORKDIR /app

COPY consumer.py .
RUN pip install kafka-python psycopg2-binary

CMD ["python", "consumer.py"]
