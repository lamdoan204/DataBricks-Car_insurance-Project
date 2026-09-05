#!/bin/bash

set -e

TOPIC="car_telematics"
PARTITIONS=2
REPLICATION_FACTOR=2

echo "======================================"
echo " Building services"
echo "======================================"

echo ""
echo "[1/6] Starting python-producer..."
docker compose up -d python-producer

echo ""
echo "[1/6] Starting kafka-1..."
docker compose up -d kafka-1

echo "[2/6] Starting kafka-2..."
docker compose up -d kafka-2

echo ""
echo "[3/6] Waiting for Kafka cluster..."

MAX_RETRIES=60
RETRY=0

while true; do

    if docker compose exec -T kafka-1 \
        /opt/kafka/bin/kafka-topics.sh \
        --bootstrap-server kafka-1:9092 \
        --list > /dev/null 2>&1; then

        echo "Kafka cluster is ready!"
        break
    fi

    RETRY=$((RETRY + 1))

    if [ "$RETRY" -ge "$MAX_RETRIES" ]; then
        echo "ERROR: Kafka startup timeout."
        exit 1
    fi

    echo "Waiting for Kafka... ($RETRY/$MAX_RETRIES)"
    sleep 2
done


echo ""
echo "[4/6] Checking topic: $TOPIC"

if docker compose exec -T kafka-1 \
    /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server kafka-1:9092 \
    --list | grep -Fxq "$TOPIC"; then

    echo "Topic '$TOPIC' already exists."

else

    echo "Topic '$TOPIC' does not exist."
    echo "Creating topic..."

    docker compose exec -T kafka-1 \
        /opt/kafka/bin/kafka-topics.sh \
        --create \
        --topic "$TOPIC" \
        --bootstrap-server kafka-1:9092 \
        --partitions "$PARTITIONS" \
        --replication-factor "$REPLICATION_FACTOR"

    echo "Topic '$TOPIC' created successfully."

fi


echo ""
echo "[5/6] Starting Kafka UI..."

docker compose up -d kafka-ui


echo ""
echo "[6/6] Starting Python Producer..."

docker compose up -d python-producer


echo ""
echo "======================================"
echo " Services ready"
echo "======================================"

docker compose ps

echo ""
echo "======================================"
echo " Kafka Topic"
echo "======================================"

docker compose exec -T kafka-1 \
    /opt/kafka/bin/kafka-topics.sh \
    --describe \
    --topic "$TOPIC" \
    --bootstrap-server kafka-1:9092


echo ""
echo "======================================"
echo " Starting Producer"
echo "======================================"

docker compose exec python-producer python main.py