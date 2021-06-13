Source: https://github.com/Marcel-Jan/docker-hadoop-spark




# Multi-containers environment with Hadoop, Hive and Spark

## Quick Start

To deploy the HDFS-Spark-Hive cluster, run:
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




## Quick Start Spark

NB: Not needed for the assignment.
```bash
  docker exec -it spark-master bash
  /spark/bin/pyspark --master spark://spark-master:7077
```

Load some .csv file into pyspark
```
  brewfile = spark.read.csv("hdfs://namenode:9000/some_data_dir/some_csv_file.csv")
  brewfile.show()
```
Go to http://<dockerhadoop_IP_address>:8080 or http://localhost:8080/ on your Docker host (laptop) to see the status of the Spark master.

