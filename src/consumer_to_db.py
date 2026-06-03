# consumer_to_db.py
import json
import time
import logging
from confluent_kafka import Consumer
import psycopg2

# time.sleep(5)

logging.basicConfig(level=logging.INFO)
logging.info("Iniciando script...")

# 0. Reintento de conexión
def connect_with_retry():
    while True:
        try:
            conn = psycopg2.connect("dbname=transacciones_db user=admin password=password123 host=postgres")
            logging.info("Conexión exitosa a la base de datos.")
            return conn
        except psycopg2.OperationalError:
            logging.warning("Base de datos no disponible. Reintentando en 5 segundos...")
            time.sleep(5)

conn = connect_with_retry()
logging.info("Esperando transacciones...")
cur = conn.cursor()

# 1. Configuración de conexión
conf = {'bootstrap.servers': 'kafka:9092', 'group.id': 'banco_group', 'auto.offset.reset': 'earliest'}
consumer = Consumer(conf)
consumer.subscribe(['transacciones-bancarias'])

# conn = psycopg2.connect("dbname=transacciones_db user=admin password=password123 host=postgres")

# print("Esperando transacciones...")

try:
    while True:
        # logging.info("Polling...")
        msg = consumer.poll(1.0)

        if msg is None: 
            continue
     
        raw_data = msg.value().decode('utf-8')
        logging.info(f"Mensaje crudo recibido: {raw_data}")
        
        # 2. Procesar mensaje
        try:
            data = json.loads(msg.value().decode('utf-8'))
            
            # 3. Insertar en Postgres
            cur.execute("INSERT INTO transacciones (usuario_id, monto) VALUES (%s, %s)", 
                        (data['usuario_id'], data['monto']))
            conn.commit()
            logging.info(f"¡ÉXITO! Transacción guardada: {data}")

        except json.JSONDecodeError:
            # Si el mensaje no es JSON, avisamos y seguimos vivos
            print(f"Error: Mensaje recibido no es un JSON válido: {msg.value()}")
            continue 
        except KeyError as e:
            # Si falta 'usuario_id' o 'monto'
            print(f"Error: El JSON no tiene el formato esperado: {e}")
            continue

except KeyboardInterrupt:
    pass

finally:
    cur.close()
    conn.close()
    consumer.close()
    logging.info("Conexiones cerradas correctamente.")