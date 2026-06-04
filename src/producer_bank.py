import time
import random
import logging
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField

logging.basicConfig(level=logging.INFO)
logging.info("Iniciando PRODUCER...")

# CONEXION productor --> KAFKA 
config={
    'bootstrap.servers': 'kafka:9092'
}
producer = Producer(config)

# SCHEMA REGISTRY
# conexion
schema_registry_client = SchemaRegistryClient({'url': 'http://schema-registry:8081'})

# cargamos diccionario
with open('/app/schemas/transaccion_schema.avsc', 'r') as f:
    schema_dic = f.read()

# serializdor
avro_serializer = AvroSerializer(schema_registry_client, schema_dic)


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
        
        # valida el mensdaje con schema_registry y convierte en binario Avro a través del serializador (topic + valor mensaje)
        payload = avro_serializer(transaccion, SerializationContext('transacciones-bancarias', MessageField.VALUE))
        
        # enviar mensaje al tópico ('transacciones-bancarias') 
        # de forma asincrona, en cola a espera en el bufer
        producer.produce(
            topic='transacciones-bancarias', 
            value=payload, 
            callback=delivery_report
        )
        
        # ejeculta el callback (delivery_report) con los mensajes de confirmacion de entrega anteriores
        producer.poll(0)

        # Esperamos 30 segundos antes de generar la siguiente transacción
        time.sleep(30)

except KeyboardInterrupt:
    logging.info("PRODUCER  detenido por el usuario.")

finally:
    # El 'flush' asegura que los datos se envíen inmediatamente y no se queden en el bufer
    producer.flush()
        
