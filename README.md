# Análisis de Datos en Tiempo Real con Apache Kafka y Spark Streaming

**Universidad Nacional Abierta y a Distancia (UNAD)**

- **Programa:** Ingeniería de Sistemas
- **Curso:** Big Data
- **Código del curso:** 202016911
- **Estudiante:** Johan Stiven Balanta Solarte
- **Actividad:** Análisis de datos en tiempo real con Apache Kafka y Streaming

## 1. Descripción del proyecto

Este proyecto corresponde a una práctica académica en la que se implementa un flujo de procesamiento de datos en tiempo real utilizando Apache Kafka, Python y Apache Spark Streaming.

Se simulan datos provenientes de sensores, incluyendo su identificador, temperatura, humedad y marca de tiempo. Posteriormente, los datos se envían a Kafka y son procesados por Spark para calcular estadísticas por sensor en ventanas de tiempo de un minuto.

## 2. Objetivo

Implementar un flujo de datos en tiempo real que permita generar, transmitir y procesar información simulada de sensores mediante herramientas de Big Data.

## 3. Tecnologías utilizadas

- **Apache Kafka:** recibe y distribuye los mensajes generados por los sensores.
- **Apache ZooKeeper:** administra los metadatos del clúster en la configuración tradicional utilizada.
- **Python:** genera los datos simulados y permite la comunicación con Kafka.
- **kafka-python:** biblioteca utilizada para enviar mensajes desde Python.
- **Apache Spark Streaming:** procesa los datos recibidos desde Kafka.
- **Spark SQL:** permite transformar los mensajes y calcular estadísticas.
- **VirtualBox:** ejecuta la máquina virtual Linux.
- **PuTTY:** permite conectarse a la máquina virtual mediante SSH.

## 4. Estructura del repositorio

```text
big-data-kafka-spark-streaming/
├── README.md
├── kafka_producer.py
├── spark_streaming_consumer.py
└── evidencias/
    ├── 01_virtualbox.png
    ├── 02_putty_ssh.png
    ├── 03_instalacion_kafka_python.png
    ├── 04_descarga_kafka.png
    ├── 05_descompresion_kafka.png
    ├── 06_carpeta_kafka.png
    ├── 07_zookeeper.png
    ├── 08_servidor_kafka.png
    ├── 09_topic_sensor_data.png
    ├── 10_creacion_topic.png
    ├── 11_codigo_productor.png
    ├── 12_archivo_productor.png
    ├── 13_codigo_consumidor.png
    ├── 14_archivo_consumidor.png
    ├── 15_ejecucion_productor.png
    ├── 16_segunda_terminal.png
    ├── 17_ejecucion_spark.png
    ├── 18_resultados_streaming.png
    ├── 19_spark_jobs.png
    ├── 20_spark_stages.png
    ├── 21_spark_environment.png
    └── 22_spark_executors.png
```

Los nombres de las evidencias pueden variar según las capturas que se hayan obtenido durante la práctica.

## 5. Procedimiento realizado

### 5.1. Preparación del entorno

Se inició la máquina virtual Linux configurada previamente con Hadoop y Spark. Luego se estableció la conexión SSH mediante PuTTY para ejecutar los comandos de instalación y configuración.

### 5.2. Instalación y configuración de Kafka

Se instaló la biblioteca `kafka-python`, se descargó Apache Kafka y se ubicaron sus archivos en `/opt/Kafka`.

También se iniciaron los servicios de ZooKeeper y Kafka, y se creó el tema `sensor_data`, destinado a almacenar los mensajes generados por los sensores.

### 5.3. Generación de datos

Se desarrolló el archivo `kafka_producer.py`, encargado de generar datos simulados con los siguientes campos:

- `sensor_id`: identificador del sensor.
- `temperature`: temperatura simulada.
- `humidity`: humedad simulada.
- `timestamp`: marca de tiempo de la medición.

El productor envía los mensajes al tema `sensor_data` de Kafka de manera continua.

### 5.4. Procesamiento de datos

Se desarrolló el archivo `spark_streaming_consumer.py` para leer los mensajes de Kafka mediante Spark Structured Streaming.

El programa interpreta los mensajes JSON, organiza los registros por sensor y por ventanas de un minuto, y calcula los promedios de temperatura y humedad.

Los resultados se configuran para mostrarse en la consola.

### 5.5. Ejecución y seguimiento

Se ejecutaron el productor y el consumidor en terminales independientes. Además, se revisó la interfaz web de Spark para observar información sobre los Jobs, Stages, Environment y Executors, según la disponibilidad de la interfaz durante la ejecución.

## 6. Comandos principales

Instalar la biblioteca de Python:

```bash
pip install kafka-python
```

Crear el tema de Kafka:

```bash
/opt/Kafka/bin/kafka-topics.sh --create \
  --bootstrap-server localhost:9092 \
  --replication-factor 1 \
  --partitions 1 \
  --topic sensor_data
```

Ejecutar el productor:

```bash
python3 kafka_producer.py
```

Ejecutar el consumidor mediante Spark:

```bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8 spark_streaming_consumer.py
```

La versión del conector de Kafka debe coincidir con la versión de Spark utilizada y estar disponible en el repositorio de dependencias.

## 7. Evidencias

Las capturas de pantalla se encuentran organizadas en la carpeta `evidencias/` y documentan las etapas de configuración, creación del tema, desarrollo de los archivos Python, ejecución del flujo de datos y seguimiento mediante la interfaz web de Spark.

### Capturas principales

- **Configuración:** inicio de VirtualBox, conexión SSH e instalación de dependencias.
- **Kafka:** inicio de servicios y creación del tema `sensor_data`.
- **Código fuente:** archivos del productor y del consumidor.
- **Ejecución:** generación y procesamiento de los datos.
- **Monitoreo:** información de Jobs, Stages, Environment y Executors.

## 8. Resultados esperados

Se espera observar mensajes generados por el productor y estadísticas calculadas por Spark para los sensores, agrupadas en ventanas de tiempo de un minuto.

La ejecución permite comprender el flujo de información entre el productor, Kafka y Spark Streaming, así como el uso de herramientas de monitoreo para revisar el procesamiento.

## 9. Conclusión

La práctica permite conocer los componentes básicos de una arquitectura de procesamiento de datos en tiempo real. Python se utiliza para simular la generación de información, Kafka para recibir y distribuir los mensajes y Spark Streaming para procesarlos y obtener estadísticas.

Este tipo de solución puede aplicarse al monitoreo de sensores, al análisis de variables ambientales y a otros escenarios que requieren procesar flujos continuos de datos.

---

**Nota:** Las evidencias y los resultados reportados deben corresponder a las actividades realmente ejecutadas. La disponibilidad de los servicios y la obtención de resultados dependen de la configuración y compatibilidad de las herramientas instaladas.
