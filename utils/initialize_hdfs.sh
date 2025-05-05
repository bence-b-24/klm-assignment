#!/bin/bash

docker exec -it klm-assignment-hadoop-namenode-1 hdfs dfs -mkdir -p /data
docker exec -it klm-assignment-hadoop-namenode-1 hdfs dfs -mkdir -p /data/archive
docker exec -it klm-assignment-hadoop-namenode-1 hdfs dfs -put -f /data/bookings /data/bookings
docker exec -it klm-assignment-hadoop-namenode-1 hdfs dfs -put -f /data/airports /data/airports
docker exec -it klm-assignment-hadoop-namenode-1 hdfs dfs -chmod -R 775 /data
docker exec -it klm-assignment-hadoop-namenode-1 hdfs dfs -chown -R airflow:airflow /data

docker exec -it klm-assignment-hadoop-namenode-1 hdfs dfs -mkdir -p /warehouse
docker exec -it klm-assignment-hadoop-namenode-1 hdfs dfs -chmod -R 775 /warehouse
docker exec -it klm-assignment-hadoop-namenode-1 hdfs dfs -chown -R airflow:airflow /warehouse