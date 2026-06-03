import json
import time
import random
import logging
from confluent_kafka import Producer

logging.basicConfig(level=logging.INFO)

# CONEXION productor --> KAFKA 
config={
    'bootstrap.servers': 'kafka:9092'
}
producer = Producer(config)

# CALLBACK (si kafka recibio el mensaje)
def delivery_report(err, msg):
    if err is not None:
        logging.error(f"Error al entregar mensaje: {err}")
    else:
        logging.info(f"Mensaje entregado con éxito a {msg.topic()} [Partición: {msg.partition()}]")
                     
logging.info("Conexión PRODUCER establecida...")

try:
    while True:
        # simulacion de dato ficticio de transacción real
        transaccion = {
            "usuario_id": random.randint(1000, 9999),
            "monto": round(random.uniform(10.5, 500.0), 2)
        }
        
        # conversion de diccionario de Python a texto (JSON) y luego a bytes
        payload = json.dumps(transaccion).encode('utf-8')
        
        # enviar mensaje al tópico ('transacciones-bancarias') 
        # de forma asincrona, en cola a espera en el bufer
        producer.produce(
            topic='transacciones-bancarias', 
            value=payload, 
            callback=delivery_report
        )
        
        # ejeculta el callback (delivery_report) con los mensajes de confirmacion de entrega anteriores
        producer.poll(0)

        # Esperamos 10 segundos antes de generar la siguiente transacción
        time.sleep(10)

except KeyboardInterrupt:
    logging.info("PRODUCER  detenido por el usuario.")

finally:
    # El 'flush' asegura que los datos se envíen inmediatamente y no se queden en el bufer
    producer.flush()
        
