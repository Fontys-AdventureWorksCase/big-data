Original: https://github.com/Marcel-Jan/docker-hadoop-spark




# Hadoop, Hive, Spark and Jupyter

## Quick Start

To deploy the HDFS-Hive-Spark-Jupyter cluster, run:
```bash
  docker-compose up -d
```

`docker-compose` creates a docker network that can be found by running `docker network list`, e.g. `hadoop_hive_spark_default`.

Run `docker network inspect` on the network (e.g. `hadoop_hive_spark_default`) to find the IP the hadoop interfaces are published on. Access these interfaces with the following URLs:

* Namenode: http://<dockerhadoop_IP_address>:9870/dfshealth.html#tab-overview
* History server: http://<dockerhadoop_IP_address>:8188/applicationhistory
* Datanode: http://<dockerhadoop_IP_address>:9864/
* Nodemanager: http://<dockerhadoop_IP_address>:8042/node
* Resource manager: http://<dockerhadoop_IP_address>:8088/
* Spark master: http://<dockerhadoop_IP_address>:8080/
* Spark worker: http://<dockerhadoop_IP_address>:8081/
* Hive: http://<dockerhadoop_IP_address>:10000

## How to Hive example
[Hive assignment](Assignment_Hive.md)

## How to Spark-Jupyter example
[Dumpert assignment](Assignment_Dumpert.md)
