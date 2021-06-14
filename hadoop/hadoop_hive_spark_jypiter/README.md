Original: https://github.com/Marcel-Jan/docker-hadoop-spark




# Hadoop, Hive, Spark and Jupyter

## Quick Start

To deploy the HDFS-Hive-Spark-Jupyter cluster, run:
```bash
  docker-compose up -d
```

Access the following ui ports in your browser:

* Zeppelin: <http://localhost:8889>
* Jupyter: <http://localhost:8888>, <http://localhost:4040> (create a Spark session in Jupyter first)
* Spark: <http://localhost:8080>, <http://localhost:8081>
* Hadoop: <http://localhost:9870>
* Neo4j: <http://localhost:7474> (TODO)
* Other ports used (see docker-compose.yml)
  Spark-master: 7077, Namenode: 9010, Datanode: 9864, History: 8188, Node-manager: 8042, Resource-manager: 8088, Hive: 10000, Hive-metastore: 9083, Presto: 8089, Neo4j: 7687


## How to Hive example
[Hive assignment](Assignment_Hive.md)

## How to Spark-Jupyter example
[Dumpert assignment](Assignment_Dumpert.md)
