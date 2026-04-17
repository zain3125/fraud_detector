FROM python:3.12-slim

WORKDIR /app

COPY consumer.py .
RUN pip install -r requirements.txt

CMD ["python", "-u", "consumer.py"]