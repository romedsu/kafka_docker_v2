FROM python:3.9-slim
WORKDIR /app
RUN pip install confluent-kafka psycopg2-binary
COPY ./src /app
RUN pip install --no-cache-dir -r requirements.txt

# se sustituye con el command: del .yml
# CMD ["python", "/app/consumer_to_db.py"] 



# FROM confluentinc/cp-kafka-connect:7.5.0

# # Instalamos el conector HTTP para obtener datos de APIs
# RUN confluent-hub install --no-prompt confluentinc/kafka-connect-http:latest