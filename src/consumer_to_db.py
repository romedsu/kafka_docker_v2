# consumer_to_db.py
import time
import logging
import psycopg2
from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField

# time.sleep(5)

logging.basicConfig(level=logging.INFO)
logging.info("Iniciando CONSUMER...")

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
cur = conn.cursor()
logging.info("Esperando transacciones...")

# 1. Configuración de conexión
conf = {'bootstrap.servers': 'kafka:9092', 'group.id': 'banco_group', 'auto.offset.reset': 'earliest'}
consumer = Consumer(conf)
consumer.subscribe(['transacciones-bancarias'])

# SCHEMA REGISTRY
# conexion
schema_registry_client = SchemaRegistryClient({'url': 'http://schema-registry:8081'})

# deserializdor
avro_deserializer = AvroDeserializer(schema_registry_client)


try:
    while True:
        # logging.info("Polling...")
        msg = consumer.poll(1.0)

        if msg is None: 
            continue
     
        transaccion = avro_deserializer(msg.value(), SerializationContext(msg.topic(),  MessageField.VALUE))

        if transaccion:
            logging.info(f"Mensaje recibido: {transaccion}")

            # 2. Procesar mensaje
            try:
               
                # 3. Insertar en Postgres
                cur.execute("INSERT INTO transacciones (usuario_id, monto) VALUES (%s, %s)", 
                            (transaccion['usuario_id'], transaccion['monto']))
                
                conn.commit()
                logging.info(f"Transacción guardada: {transaccion}\n")

            except KeyError as e:
                # Si falta 'usuario_id' o 'monto'
                logging.error(f"Error de formato: El esquema no coincide con lo esperado: {e}")
            except Exception as e:
                # Captura errores de deserialización (ej: mensaje corrupto, esquema no encontrado)
                logging.error(f"Error crítico al procesar mensaje: {e}")
                continue
             
except KeyboardInterrupt:
    pass

finally:
    cur.close()
    conn.close()
    consumer.close()
    logging.info("Conexiones cerradas correctamente.")