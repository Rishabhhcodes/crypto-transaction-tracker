FROM python:3.10-slim   

WORKDIR /app

COPY . .

RUN pip install --no-cache-dir kafka-python requests

CMD ["python", "-u", "producer.py"]