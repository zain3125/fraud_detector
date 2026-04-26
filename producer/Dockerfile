FROM python:3.12-slim

WORKDIR /app

COPY producer/requirements.txt .
RUN pip install --upgrade pip && pip install -r requirements.txt

COPY utils/ ./utils/

COPY producer/ .

CMD ["python", "-u", "producer.py"]
