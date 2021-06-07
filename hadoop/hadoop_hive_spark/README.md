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

## Quick Start HDFS

This loads the assignment files into hdfs.
```bash
  docker exec -it namenode bash
  hadoop fs -mkdir /ml_data
  hadoop fs -put /import/ml_data /ml_data
```


## Assignment Hive

Go to the command line of the Hive server and start hiveserver2

```bash
docker exec -it hive-server bash
Hiveserver2 # I believe you can skip this line
beeline -u jdbc:hive2://localhost:10000 -n root
```

In beeline write
```sql
CREATE DATABASE movielens;
USE movielens;
```
### Create and fill ratings table
```sql
DROP TABLE IF EXISTS ratings;

CREATE TABLE ratings (user_id SMALLINT, movie_id SMALLINT, rating TINYINT, stamp INT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE;

LOAD DATA INPATH '/ml_data/ml_data/u.data' OVERWRITE INTO TABLE ratings;

SELECT * FROM ratings LIMIT 10;
```
NB: Stamp column can be converted to TIMESTAMP after import\
Quick example: `create table mytime(a string, b timestamp);` `insert into table mytime select a, from_unixtime(unix_timestamp(b, 'dd-MM-yyyy HH:mm')) from tmp;`\
Also check: https://cwiki.apache.org/confluence/display/Hive/LanguageManual+UDF#LanguageManualUDF-DateFunctions


### Create and fill movies table
```sql
DROP TABLE IF EXISTS movies;

CREATE TABLE movies (movie_id SMALLINT, title STRING, released STRING, video_released STRING, imdb_url STRING, genre_unknown BINARY, genre_action BINARY, genre_adventure BINARY, genre_animation BINARY, genre_childrens BINARY, genre_comedy BINARY, genre_crime BINARY, genre_documentary BINARY, genre_drama BINARY, genre_fantasy BINARY, genre_filmnoir BINARY, genre_horror BINARY, genre_musical BINARY, genre_mystery BINARY, genre_romance BINARY, genre_scifi BINARY, genre_thriller BINARY, genre_war BINARY, genre_western BINARY)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS TEXTFILE;

LOAD DATA INPATH '/ml_data/ml_data/u.item' OVERWRITE INTO TABLE movies;

SELECT * FROM movies LIMIT 10;
```
NB1: two released columns need to be converted to datatype DATE after import\
NB2: BINARY can be converted to BOOLEAN after import


### Create and fill users table
```sql
DROP TABLE IF EXISTS users;

CREATE TABLE users ( user_id SMALLINT, age TINYINT, gender VARCHAR(1), occupation STRING, zip_code INT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS TEXTFILE;

LOAD DATA INPATH '/ml_data/ml_data/u.user' OVERWRITE INTO TABLE users;

SELECT * FROM users LIMIT 10;
```

### Questions

1. 
   ```sql
   SELECT COUNT(*) FROM users
   WHERE gender = 'M';
   ```
   670
   ```sql
   SELECT COUNT(*) FROM users
   WHERE gender = 'F';
   ```
   273

2. ```sql
   SELECT occupation, gender, count(*) FROM users
   GROUP BY occupation, gender
   ORDER BY occupation, gender
   ```
   ```
   +----------------+---------+------+
   |   occupation   | gender  | _c2  |
   +----------------+---------+------+
   | administrator  | F       | 36   |
   | administrator  | M       | 43   |
   | artist         | F       | 13   |
   | artist         | M       | 15   |
   | doctor         | M       | 7    |
   | educator       | F       | 26   |
   […]
   ```
   
3. ```sql
   SELECT m.movie_id, m.title, u.gender, AVG(r.rating) as average_rating, count(r.rating) as n_ratings FROM movies m 
   JOIN ratings r ON r.movie_id = m.movie_id
   JOIN users u ON u.user_id = r.user_id
   GROUP BY m.movie_id, title, u.gender
   ORDER BY average_rating DESC
   LIMIT 10
   ```
   
   There are several candidates, but it's doubtful whether the results are worth anything, because these high averages are based on very few ratings. IMDB calculates weighted average ratings using statistical methods. They do not disclose their exact calculations to prevent abuse.
   
   ```
   +-------------+-----------------------------------------+-----------+-----------------+------------+
   | m.movie_id  |                 m.title                 | u.gender  | average_rating  | n_ratings  |
   +-------------+-----------------------------------------+-----------+-----------------+------------+
   | 883         | Telling Lies in America (1997)          | F         | 5.0             | 1          |
   | 1175        | Hugo Pool (1997)                        | M         | 5.0             | 2          |
   | 1605        | Love Serenade (1996)                    | M         | 5.0             | 1          |
   | 1144        | Quiet Room, The (1996)                  | M         | 5.0             | 1          |
   | 884         | Year of the Horse (1997)                | F         | 5.0             | 1          |
   | 119         | Maya Lin: A Strong Clear Vision (1994)  | F         | 5.0             | 1          |
   | 814         | Great Day in Harlem, A (1994)           | M         | 5.0             | 1          |
   | 1189        | Prefontaine (1997)                      | M         | 5.0             | 2          |
   | 1656        | Little City (1998)                      | M         | 5.0             | 1          |
   | 74          | Faster Pussycat! Kill! Kill! (1965)     | F         | 5.0             | 1          |
   +-------------+-----------------------------------------+-----------+-----------------+------------+
   ```

4. (optional)
   ```sql
   SELECT m.movie_id, m.title, u.gender, AVG(r.rating) as average_rating FROM movies m 
   JOIN ratings r ON r.movie_id = m.movie_id
   JOIN users u ON u.user_id = r.user_id
   GROUP BY m.movie_id, title, u.gender, m.genre_action, m.genre_romance, m.genre_horror 
   HAVING m.genre_action = 1 OR m.genre_romance = 1 OR m.genre_horror = 1
   ORDER BY average_rating DESC
   LIMIT 10
   ```
   This gave an execution error. I stopped here since it was optional.
   


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

