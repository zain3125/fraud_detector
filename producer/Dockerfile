FROM python:3.12-slim

WORKDIR /app

COPY producer.py .
RUN pip install -r requirements.txt

CMD ["python", "-u", "producer.py"]
