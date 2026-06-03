FROM python:3.9-slim
WORKDIR /app

RUN apt-get update && apt-get install -y gcc libpq-dev && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# RUN pip install confluent-kafka psycopg2-binary
COPY ./src /app
COPY ./schemas /app/schemas

# se sustituye con el command: del .yml
# CMD ["python", "/app/consumer_to_db.py"] 



