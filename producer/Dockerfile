FROM python:3.12-slim

WORKDIR /app

COPY producer.py .
RUN pip install kafka-python faker

CMD ["python", "-u", "producer.py"]
