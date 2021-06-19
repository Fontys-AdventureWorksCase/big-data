```python
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
import datetime

# Start spark session
startTime = datetime.datetime.now()

spark = SparkSession \
    .builder\
    .appName("Anime importer") \
    .master("spark://spark-master:7077") \
    .config("spark.driver.memory", "15g") \
    .config("spark.jars", "/extra_jars/neo4j-connector-apache-spark_2.12-4.0.1_for_spark_3.jar") \
    .config("neo4j.url", "bolt://neo4j:7687")\
    .config("neo4j.authentication.type", "basic")\
    .config("neo4j.authentication.basic.username", "neo4j")\
    .config("neo4j.authentication.basic.password", "password")\
    .getOrCreate()

print(datetime.datetime.now()-startTime)
```

    0:00:03.617098



```python
# read users
startTime = datetime.datetime.now()

data_file = '/import/anime/users_cleaned.csv' 

users = spark.read.csv(data_file, header=True, sep=",", inferSchema=True).cache()
print('Csv users = {}'.format(users.count()))
users.printSchema()
users.show(1, vertical=True)

print(datetime.datetime.now()-startTime)

# write users to neo4j
startTime = datetime.datetime.now()

users.write.format("org.neo4j.spark.DataSource") \
    .option("node.keys", "username")\
    .option("schema.optimization.type", "INDEX")\
    .mode("Overwrite")\
    .option("labels", ":User") \
    .save()

print(datetime.datetime.now()-startTime)
```

    Csv users = 108712
    root
     |-- username: string (nullable = true)
     |-- user_id: integer (nullable = true)
     |-- user_watching: integer (nullable = true)
     |-- user_completed: integer (nullable = true)
     |-- user_onhold: integer (nullable = true)
     |-- user_dropped: integer (nullable = true)
     |-- user_plantowatch: integer (nullable = true)
     |-- user_days_spent_watching: double (nullable = true)
     |-- gender: string (nullable = true)
     |-- location: string (nullable = true)
     |-- birth_date: string (nullable = true)
     |-- access_rank: string (nullable = true)
     |-- join_date: string (nullable = true)
     |-- last_online: string (nullable = true)
     |-- stats_mean_score: string (nullable = true)
     |-- stats_rewatched: string (nullable = true)
     |-- stats_episodes: double (nullable = true)
    
    -RECORD 0---------------------------------------
     username                 | karthiga            
     user_id                  | 2255153             
     user_watching            | 3                   
     user_completed           | 49                  
     user_onhold              | 1                   
     user_dropped             | 0                   
     user_plantowatch         | 0                   
     user_days_spent_watching | 55.09166666666667   
     gender                   | Female              
     location                 | Chennai, India      
     birth_date               | 1990-04-29 00:00:00 
     access_rank              | null                
     join_date                | 2013-03-03 00:00:00 
     last_online              | 2014-02-04 01:32:00 
     stats_mean_score         | 7.43                
     stats_rewatched          | 0.0                 
     stats_episodes           | 3391.0              
    only showing top 1 row
    
    0:00:08.152396
    0:00:04.073273



```python
# read animes
startTime = datetime.datetime.now()

data_file = '/import/anime/anime_cleaned.csv' 

animes = spark.read.csv(data_file, header=True, sep=",", inferSchema=True).cache() 
print('Total Records = {}'.format(animes.count()))
animes.printSchema()
animes.show(1, vertical=True)

print(datetime.datetime.now()-startTime)

# write animes to neo4j
startTime = datetime.datetime.now()

animes.write.format("org.neo4j.spark.DataSource") \
    .option("node.keys", "anime_id")\
    .option("schema.optimization.type", "INDEX")\
    .mode("Overwrite")\
    .option("labels", ":Anime") \
    .save()

print(datetime.datetime.now()-startTime)
```

    Total Records = 6668
    root
     |-- anime_id: integer (nullable = true)
     |-- title: string (nullable = true)
     |-- title_english: string (nullable = true)
     |-- title_japanese: string (nullable = true)
     |-- title_synonyms: string (nullable = true)
     |-- image_url: string (nullable = true)
     |-- type: string (nullable = true)
     |-- source: string (nullable = true)
     |-- episodes: string (nullable = true)
     |-- status: string (nullable = true)
     |-- airing: string (nullable = true)
     |-- aired_string: string (nullable = true)
     |-- aired: string (nullable = true)
     |-- duration: string (nullable = true)
     |-- rating: string (nullable = true)
     |-- score: string (nullable = true)
     |-- scored_by: string (nullable = true)
     |-- rank: double (nullable = true)
     |-- popularity: double (nullable = true)
     |-- members: double (nullable = true)
     |-- favorites: integer (nullable = true)
     |-- background: string (nullable = true)
     |-- premiered: string (nullable = true)
     |-- broadcast: string (nullable = true)
     |-- related: string (nullable = true)
     |-- producer: string (nullable = true)
     |-- licensor: string (nullable = true)
     |-- studio: string (nullable = true)
     |-- genre: string (nullable = true)
     |-- opening_theme: string (nullable = true)
     |-- ending_theme: string (nullable = true)
     |-- duration_min: string (nullable = true)
     |-- aired_from_year: string (nullable = true)
    
    -RECORD 0-------------------------------
     anime_id        | 11013                
     title           | Inu x Boku SS        
     title_english   | Inu X Boku Secret... 
     title_japanese  | 妖狐×僕SS            
     title_synonyms  | Youko x Boku SS      
     image_url       | https://myanimeli... 
     type            | TV                   
     source          | Manga                
     episodes        | 12                   
     status          | Finished Airing      
     airing          | False                
     aired_string    | Jan 13, 2012 to M... 
     aired           | {'from': '2012-01... 
     duration        | 24 min. per ep.      
     rating          | PG-13 - Teens 13 ... 
     score           | 7.63                 
     scored_by       | 139250               
     rank            | 1274.0               
     popularity      | 231.0                
     members         | 283882.0             
     favorites       | 2809                 
     background      | Inu x Boku SS was... 
     premiered       | Winter 2012          
     broadcast       | Fridays at Unknown   
     related         | {'Adaptation': [{... 
     producer        | Aniplex, Square E... 
     licensor        | Sentai Filmworks     
     studio          | David Production     
     genre           | Comedy, Supernatu... 
     opening_theme   | "['""Nirvana"" by... 
     ending_theme    | "['#1: ""Nirvana"... 
     duration_min    |  11-12)'             
     aired_from_year |  '#2: ""Rakuen no... 
    only showing top 1 row
    
    0:00:01.003258
    0:00:01.334324



```python
# get schema
sample_file = '/import/anime/animelists_cleaned_sample.csv' 

sample = (spark.read.csv(sample_file, 
                         header=True, 
                         sep=",", 
                         ignoreLeadingWhiteSpace=True, 
                         ignoreTrailingWhiteSpace=True, 
                         dateFormat="yyyy-MM-dd", 
                         timestampFormat="yyyy-MM-dd HH:mm:ss",
                         inferSchema=True
                         ).cache())
sample.printSchema()

# read relationships
startTime = datetime.datetime.now()

data_file = '/import/anime/animelists_cleaned.csv' 

relationships = (spark.read.csv(data_file, 
                               header=True, 
                               sep=",", 
                               ignoreLeadingWhiteSpace=True, 
                               ignoreTrailingWhiteSpace=True, 
                               dateFormat="yyyy-MM-dd",
                               timestampFormat="yyyy-MM-dd HH:mm:ss",
                               schema=sample.schema, 
                               mode="DROPMALFORMED"
                               ).cache())
relationships.show(2)

print('Records read = {}'.format(relationships.count()))
print(datetime.datetime.now()-startTime)

previousDbSize=(spark.read.format("org.neo4j.spark.DataSource")
 .option("url", "bolt://neo4j:7687")
 .option("query", "MATCH (n) RETURN n")
 .load().count())
```

    root
     |-- username: string (nullable = true)
     |-- anime_id: integer (nullable = true)
     |-- my_watched_episodes: integer (nullable = true)
     |-- my_start_date: string (nullable = true)
     |-- my_finish_date: string (nullable = true)
     |-- my_score: integer (nullable = true)
     |-- my_status: integer (nullable = true)
     |-- my_rewatching: double (nullable = true)
     |-- my_rewatching_ep: integer (nullable = true)
     |-- my_last_updated: timestamp (nullable = true)
     |-- my_tags: string (nullable = true)
    
    +--------+--------+-------------------+-------------+--------------+--------+---------+-------------+----------------+-------------------+-------+
    |username|anime_id|my_watched_episodes|my_start_date|my_finish_date|my_score|my_status|my_rewatching|my_rewatching_ep|    my_last_updated|my_tags|
    +--------+--------+-------------------+-------------+--------------+--------+---------+-------------+----------------+-------------------+-------+
    |karthiga|      21|                586|   0000-00-00|    0000-00-00|       9|        1|         null|               0|2013-03-03 10:52:53|   null|
    |karthiga|      59|                 26|   0000-00-00|    0000-00-00|       7|        2|         null|               0|2013-03-10 13:54:51|   null|
    +--------+--------+-------------------+-------------+--------------+--------+---------+-------------+----------------+-------------------+-------+
    only showing top 2 rows
    
    Records read = 31279481
    0:00:45.569248



```python
# repartition
startTime = datetime.datetime.now()

# https://neo4j.com/developer/spark/faq/#_my_writes_are_failing_due_to_deadlock_exceptions
partitioned = relationships.repartition(1, "username", "anime_id") 
print(partitioned.rdd.getNumPartitions())
partitioned.show()


print('Repartitioning took {}'.format(datetime.datetime.now()-startTime))

# write relationships to neo4j
startTime = datetime.datetime.now()

(partitioned.write.format("org.neo4j.spark.DataSource") 
    .mode("Overwrite")
#     .option("batch.size", 40000) # bigger size is less transactional overhead == faster, but watch for out of memory errors
    .option("relationship.properties", "my_watched_episodes,my_start_date,my_finish_date,my_score,my_status,my_rewatching,my_rewatching_ep,my_last_updated,my_tags")
    .option("relationship", "WATCHED") 
    .option("relationship.save.strategy", "keys") 
    .option("relationship.source.save.mode", "Match")
    .option("relationship.source.labels", ":User")
    .option("relationship.source.node.keys", "username")
    .option("relationship.target.save.mode", "Match")
    .option("relationship.target.labels", ":Anime")
    .option("relationship.target.node.keys", "anime_id")
    .save())

currentDbSize=(spark.read.format("org.neo4j.spark.DataSource")
 .option("url", "bolt://neo4j:7687")
 .option("query", "MATCH (n) RETURN n")
 .load().count())

insertCount = previousDbSize-currentDbSize
print('Inserted Records = {}'.format(insertCount))
print(datetime.datetime.now()-startTime)
```

    1
    +--------+--------+-------------------+-------------+--------------+--------+---------+-------------+----------------+-------------------+-------+
    |username|anime_id|my_watched_episodes|my_start_date|my_finish_date|my_score|my_status|my_rewatching|my_rewatching_ep|    my_last_updated|my_tags|
    +--------+--------+-------------------+-------------+--------------+--------+---------+-------------+----------------+-------------------+-------+
    |karthiga|      21|                586|   0000-00-00|    0000-00-00|       9|        1|         null|               0|2013-03-03 10:52:53|   null|
    |karthiga|      59|                 26|   0000-00-00|    0000-00-00|       7|        2|         null|               0|2013-03-10 13:54:51|   null|
    |karthiga|      74|                 26|   0000-00-00|    0000-00-00|       7|        2|         null|               0|2013-04-27 16:43:35|   null|
    |karthiga|     120|                 26|   0000-00-00|    0000-00-00|       7|        2|         null|               0|2013-03-03 10:53:57|   null|
    |karthiga|     178|                 26|   0000-00-00|    0000-00-00|       7|        2|          0.0|               0|2013-03-27 15:59:13|   null|
    |karthiga|     210|                161|   0000-00-00|    0000-00-00|       7|        2|         null|               0|2013-03-10 13:57:06|   null|
    |karthiga|     232|                 70|   0000-00-00|    0000-00-00|       6|        2|         null|               0|2013-03-09 17:24:42|   null|
    |karthiga|     233|                 78|   0000-00-00|    0000-00-00|       6|        2|         null|               0|2013-03-10 05:29:44|   null|
    |karthiga|     249|                167|   0000-00-00|    0000-00-00|       8|        2|         null|               0|2013-03-19 16:04:46|   null|
    |karthiga|     269|                366|   0000-00-00|    0000-00-00|      10|        2|         null|               0|2013-03-03 09:39:23|   null|
    |karthiga|     721|                 38|   0000-00-00|    0000-00-00|       7|        2|         null|               0|2013-04-27 16:23:53|   null|
    |karthiga|     853|                 26|   0000-00-00|    0000-00-00|       7|        2|         null|               0|2013-03-10 13:57:27|   null|
    |karthiga|     857|                 25|   0000-00-00|    0000-00-00|      10|        2|          0.0|               0|2013-03-03 13:19:55|   null|
    |karthiga|     957|                 39|   0000-00-00|    0000-00-00|       8|        2|         null|               0|2013-04-27 16:34:18|   null|
    |karthiga|     966|                506|   0000-00-00|    0000-00-00|      10|        1|         null|               0|2013-03-10 14:29:59|   null|
    |karthiga|    1557|                 26|   0000-00-00|    0000-00-00|       8|        2|         null|               0|2013-05-05 14:38:12|   null|
    |karthiga|    1571|                 25|   0000-00-00|    0000-00-00|       8|        2|         null|               0|2013-04-27 16:31:37|   null|
    |karthiga|    1579|                 25|   0000-00-00|    0000-00-00|       9|        2|         null|               0|2013-04-27 16:44:33|   null|
    |karthiga|    1698|                 23|   0000-00-00|    0000-00-00|       7|        2|         null|               0|2013-04-27 16:14:27|   null|
    |karthiga|    1735|                303|   0000-00-00|    0000-00-00|       9|        1|         null|               0|2013-03-10 13:37:49|   null|
    +--------+--------+-------------------+-------------+--------------+--------+---------+-------------+----------------+-------------------+-------+
    only showing top 20 rows
    
    Repartitioning took 0:00:03.844707
    Inserted Records = 0
    1:10:46.220079



```python

```


```python

```
