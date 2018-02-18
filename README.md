# Your Best Blood Pressure Friend
Your best blood pressure friend will inform you that your systolic and diastolic pressures are normal, hypotensive, or hypertensive.

# Installing
PySpark, Kafka

# Running the programs
## Start Zookeeper Server
KAFKA_HEAP_OPTS="-Xmx32M" bin/zookeeper-server-start.sh config/zookeeper.properties > /tmp/zookeeper.log 2>&1 &

## Start Kafka server
KAFKA_HEAP_OPTS="-Xmx200M" bin/kafka-server-start.sh config/server.properties > /tmp/kafka.log 2>&1 &

## Run the Kafka Direct Stream
#### Open an another terminal to execute this command for processing IoT data
KAFKA_HEAP_OPTS="-Xmx1M" spark-submit --jars spark-streaming-kafka-0-8-assembly_2.11-2.0.0-preview.jar ./kafka-direct-iot-bp-sql.py localhost:9092 iotmsgs

## Run IoT Device Data Simulator
#### Send JSON messages
KAFKA_HEAP_OPTS="-Xmx20M" ./iotsimulator_bp.py 20 | /home/ec2-user/LAB6/kafka_2.11-0.10.1.0/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic iotmsgs

# Simulation Result
[SimulationResult.pdf](https://github.com/hubert39/IoT/blob/master/IOT_FINAL.pdf)
