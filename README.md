# LakeHouse For Data Streaming And Analysis:
## The real time streaming system for coin analysis
![Image](./md%20img//architecture.png)
# Quick setup
## Requirement : Git & Docker
### [GIT](https://git-scm.com/book/en/v2/Getting-Started-Installing-Git)
### [Docker](https://www.docker.com/products/docker-desktop/)
## pull repo to local
```
git clone https://github.com/gdckpvp/Streaming_Coin.git
cd Streaming_Coin
docker compose up
```
# Port to connect Service
## Minio
```
Minio API:     http://localhost:9000
Minio Console: http://localhost:9001
UserName: minioadmin
PassWord: minioadmin
```
## Superset
```
Superset Console: http://localhost:8088
UserName: admin
PassWord: admin
```
## Trino
```
Trino Console: http://localhost:8080
UserName: Trino
```
## Spark Streaming
```
Spark Console: http://localhost:4040
```
# Kafka Broker
```
Kafka Broker: http://localhost:9092
```
## Jupiter Notebook
```
Jupiter Console: http://localhost:8888
```
# Note
#### After running docker compose up you should check and restart the service Spark-streaming and Producer, because the first time you run docker compose up there is no bucket or data in Minio-Bucket so the service will auto turn off.
#### After setup connection string for superset to trino and add the dashboard in "./data/Superset Dashboard" you will have this dashboard in superset
![alt text](./md%20img/image.png)
