# Sistema de Ingesta de Datos en Tiempo Real (Kafka + Postgres + Python (producer + consumer)) --> V2

## Sistema de ingesta de datos a través de un pipeline de procesamiento de datos en streaming en tiempo real, desplegado sobre Docker a través de Apache Kafka y PostgreSQL.

## De forma manual se introducen datos (transacciones bancarias ficticias) para a través de Kafka, captar el evento y registrarlo en la BBDD de Postgre.

## CREAR TOPIC
```docker exec -it kafka_docker_v1-kafka-1 kafka-topics --create --topic transacciones-bancarias --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1 ```

```docker exec -it kafka kafka-topics --create --topic transacciones-bancarias --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1```

## CREAR | VER PRODUCTOR
```docker exec -it kafka_docker_v2-kafka-1 kafka-console-producer --bootstrap-server localhost:9092 --topic transacciones-bancarias```

```docker exec -it kafka kafka-console-producer --bootstrap-server kafka:9092 --topic transacciones-bancarias```

```docker logs -f python-producer```

## CREAR | VER CONSUMIDOR (otra terminal)
```docker exec -it kafka kafka-console-consumer -bootstrap-server localhost:9092 --topic transacciones-bancarias --from-beginning```

```docker logs -f python-consumer```

## VER LISTA TOPICS creados
```docker exec kafka_docker_v2-kafka-1 kafka-topics --list --bootstrap-server localhost:9092```

## LOGS (consumer)
```docker logs -f python-consumer``

## CONSULTA BBDD
```docker exec -it postgres psql -U admin -d transacciones_db -c "SELECT * FROM transacciones;"```

---
## ORDEN SECUENCIA ARRANQUE
healthcheck --> comprobar que cada servicio ha arrancado correctamente y devuelve respuesta

1. ZooKeeper: Es el primero. Sin él, Kafka no arranca.

2. Kafka: Espera a zookeeper.

3. Schema Registry: Espera a kafka (usando healthcheck).

4. Base de Datos (Postgres): Puede arrancar en paralelo a Kafka.

5. Productor y Consumidor: Son los últimos; esperan a que todo lo anterior sea healthy.


## SQL --> init-scripts--> scripts .sql de creación tablas de bbdd

## Python --> Data Ingestion Pipeline --> script .py para insertar datos en bbdd

## Python --> Data PRODUCER Pipeline --> script .py para producir datos y enviar al broker de kafka
1. Generación: random crea el dato.

2. Producción: produce() lo mueve al área de salida (buffer).

3. Confirmación: El broker kakfa recibe y responde; la respuesta (éxito/error) se añade en cola en el buffer de respuestas interno de la librería.

4. Ejecución: poll() saca esas respuestas de la cola y ejecuta el callback delivery_report.

5. Pausa: sleep() tiempo de pausa entre transaccion.

6. Garantía: flush() asegura el envío de todas transacciones pendientes en el buffer de salida.

## SCHEMA REGISTRY --> Data Governance --> Garantizar formato correcto de los datos --> AVRO .avsc
