FROM python:3.12-slim

WORKDIR /app

COPY consumer/requirements.txt .
RUN pip install --upgrade pip && pip install -r requirements.txt

COPY utils/ ./utils/

COPY consumer/ .

CMD ["python", "-u", "consumer.py"]
