#!/bin/bash

set -e

sudo yum update -y

sudo yum install -y java-17-amazon-corretto-devel wget tar python3-pip
sudo yum install -y redis6 || sudo yum install -y redis

sudo pip3 install kafka-python redis boto3 psycopg2-binary

MASTER_IP=$(hostname -I | awk '{print $1}')

cd /opt

if [ ! -d /opt/kafka_2.12-3.7.0 ]; then
  sudo wget -q https://archive.apache.org/dist/kafka/3.7.0/kafka_2.12-3.7.0.tgz
  sudo tar -xzf kafka_2.12-3.7.0.tgz
fi

sudo ln -sfn /opt/kafka_2.12-3.7.0 /opt/kafka

sudo mkdir -p /mnt/kafka-logs
sudo mkdir -p /mnt/zookeeper

sudo tee /opt/kafka/config/zookeeper.properties > /dev/null <<EOF
dataDir=/mnt/zookeeper
clientPort=2181
maxClientCnxns=0
admin.enableServer=false
EOF

sudo tee /opt/kafka/config/server.properties > /dev/null <<EOF
broker.id=1
listeners=PLAINTEXT://0.0.0.0:9092
advertised.listeners=PLAINTEXT://${MASTER_IP}:9092
num.network.threads=3
num.io.threads=8
socket.send.buffer.bytes=102400
socket.receive.buffer.bytes=102400
socket.request.max.bytes=104857600
log.dirs=/mnt/kafka-logs
num.partitions=6
default.replication.factor=1
min.insync.replicas=1
log.retention.hours=24
log.segment.bytes=1073741824
zookeeper.connect=localhost:2181
zookeeper.connection.timeout.ms=18000
group.initial.rebalance.delay.ms=0
auto.create.topics.enable=true
delete.topic.enable=true
EOF

sudo tee /etc/systemd/system/zookeeper.service > /dev/null <<EOF
[Unit]
Description=Apache Zookeeper
After=network.target

[Service]
Type=simple
ExecStart=/opt/kafka/bin/zookeeper-server-start.sh /opt/kafka/config/zookeeper.properties
ExecStop=/opt/kafka/bin/zookeeper-server-stop.sh
Restart=on-failure
User=root

[Install]
WantedBy=multi-user.target
EOF

sudo tee /etc/systemd/system/kafka.service > /dev/null <<EOF
[Unit]
Description=Apache Kafka
After=zookeeper.service
Requires=zookeeper.service

[Service]
Type=simple
ExecStart=/opt/kafka/bin/kafka-server-start.sh /opt/kafka/config/server.properties
ExecStop=/opt/kafka/bin/kafka-server-stop.sh
Restart=on-failure
User=root

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable zookeeper
sudo systemctl enable kafka

sudo systemctl start zookeeper
sleep 15

sudo systemctl start kafka
sleep 20

/opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server ${MASTER_IP}:9092 \
  --create \
  --if-not-exists \
  --topic omniroute.telemetry.bronze \
  --partitions 6 \
  --replication-factor 1

/opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server ${MASTER_IP}:9092 \
  --create \
  --if-not-exists \
  --topic omniroute.telemetry.silver \
  --partitions 6 \
  --replication-factor 1

REDIS_SERVER=$(command -v redis-server || command -v redis6-server || true)
REDIS_CLI=$(command -v redis-cli || command -v redis6-cli || true)

if [ -z "$REDIS_SERVER" ]; then
  echo "redis-server not found"
  exit 1
fi

if [ -z "$REDIS_CLI" ]; then
  echo "redis-cli not found"
  exit 1
fi

sudo mkdir -p /etc/redis
sudo mkdir -p /mnt/redis

sudo tee /etc/redis/redis.conf > /dev/null <<EOF
bind 0.0.0.0
protected-mode no
port 6379
dir /mnt/redis
dbfilename dump.rdb
save 300 1
appendonly yes
daemonize no
supervised systemd
logfile ""
EOF

sudo tee /etc/systemd/system/redis.service > /dev/null <<EOF
[Unit]
Description=Redis Server
After=network.target

[Service]
Type=notify
ExecStart=${REDIS_SERVER} /etc/redis/redis.conf --supervised systemd
ExecStop=${REDIS_CLI} -p 6379 shutdown
Restart=on-failure
User=root

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable redis
sudo systemctl restart redis

sleep 5

${REDIS_CLI} -h ${MASTER_IP} -p 6379 ping

echo "Kafka and Redis setup complete on ${MASTER_IP}"