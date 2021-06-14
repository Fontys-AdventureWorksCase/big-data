# Assignment Dumpert

## Spark

You could directly open PySpark and run commands

```bash
  docker exec -it spark-master bash
  /spark/bin/pyspark --master spark://spark-master:7077
```

Load some .csv file into PySpark

```bash
  brewfile = spark.read.csv("hdfs://namenode:9000/some_data_dir/some_csv_file.csv")
  brewfile.show()
```

Go to <http://localhost:8080/> on your Docker host (laptop) to see the status of the Spark master.

## Jupyter

I added these lines to the `docker-compose.yml` to add Jupyter

```json
  jupyterlab:
    image: andreper/jupyterlab:3.0.0-spark-3.0.0
    container_name: jupyterlab
    ports:
      - 8888:8888
      - 4040:4040
    volumes:
      - ./notebooks:/opt/workspace
```

Note: I've also included a Zeppelin container, but eventually decided to use Jupyter.

## Import files into HDFS

```bash
docker-compose exec -it namenode bash
Hadoop fs -mkdir /dumpert
Hadoop fs -put /import/dumpert/ /dumpert
```

NB: These files are not included on Github. Download them from the course material. It takes at least a few hours to import!

## Write PySpark code in Jupyter

To access Jupyter go to <http://localhost:8888/>.

Scroll down to find the jupyter notebook. (The most up-to-date version is available in folder Notebooks in its original format.)

## Batch processing

I had to set driver memory to 15GB or it would time out. (NB: It’s best practice to minimize the amount of collect operations 
if possible or use a smaller subset of the data to collect during data exploration. From <https://key2consulting.com/boost-query-performance-databricks-spark/> )

![image](https://user-images.githubusercontent.com/4932561/121825077-d1bbfa80-ccb0-11eb-9934-9fb7169c02a9.png)

This is what the Spark WebUI shows when loading 5.4 gb of comments into a dataframe.

![image](https://user-images.githubusercontent.com/4932561/121825075-c963bf80-ccb0-11eb-9b94-1d53d0fb4ebe.png)

![image](https://user-images.githubusercontent.com/4932561/121868042-d3151380-cd00-11eb-8f54-4b58f53ff4b7.png)

## Optimizations

### Use caching
Caching will provide significant performance improvement while running several queries on the same dataframe. \
From <https://key2consulting.com/boost-query-performance-databricks-spark/> 

  With: \
  ![image](https://user-images.githubusercontent.com/4932561/121825162-219ac180-ccb1-11eb-96f8-a81470889a3b.png)
  
  Without: \
  ![image](https://user-images.githubusercontent.com/4932561/121825172-2e1f1a00-ccb1-11eb-837e-3561e47a00ab.png)
  
  With: \
  ![image](https://user-images.githubusercontent.com/4932561/121825185-39724580-ccb1-11eb-9528-5e3c99fe93e3.png) \
  (Ignore count 15)
  
  Without: \
  ![image](https://user-images.githubusercontent.com/4932561/121825190-3ecf9000-ccb1-11eb-821c-1323fb75297c.png)

### Minimize actions
I have tried to keep actions to a minimum. Actions are operations which take DataFrame(s) as input and output something else. With Spark, operations are added to the graph describing what Spark should eventually do. When an action is requested – e.g. display the contents of this Spark DataFrame – Spark looks at the processing graph and then optimizes the tasks which needs to be done. From <https://data.solita.fi/pyspark-execution-logic-code-optimization/> 

### Partitioning optimization
I have not customized partioning for this assignment. However, partioning is a crucial part of the optimization toolbox. 

### Write interim data
It’s good practice to write a few interim tables that will be used by several users or queries on a regular basis. \
From <https://key2consulting.com/boost-query-performance-databricks-spark/> 


# Jupyter notebook

## Start session

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import datetime

# Start spark session
startTime = datetime.datetime.now()

spark = SparkSession \
        .builder \
        .appName("Streaming") \
        .config("spark.driver.memory", "15g") \
        .master("spark://spark-master:7077") \
        .getOrCreate()

print(datetime.datetime.now()-startTime)
```

    0:00:03.856022


## Single file processing (test)


```python
startTime = datetime.datetime.now()

# Check if files are interpreted correctly

staticVideosTest = spark.read \
    .json("hdfs://namenode:9000/dumpert/videos/9999") \
    .select(explode("items").alias("videos")) \
    .select( \
      col("videos.date").alias("date") \
    , col("videos.description").alias("description") \
    , col("videos.id").alias("id") \
    , col("videos.media").alias("media") \
    , col("videos.nopreroll").alias("nopreroll") \
    , col("videos.nsfw").alias("nsfw") \
    , col("videos.stats").alias("stats") \
    , col("videos.still").alias("still") \
    , col("videos.stills").alias("stills") \
    , col("videos.tags").alias("tags") \
    , col("videos.thumbnail").alias("thumbnail") \
    , col("videos.title").alias("title")
    )

staticVideosTest.show(1, vertical=True)
# parentDF.printSchema()

print(datetime.datetime.now()-startTime)
```

    -RECORD 0---------------------------
     date        | 2008-03-01 07:12:41
     description | "Conjonedak or so...
     id          | 43365_47842255
     media       | [[, 209, VIDEO, [...
     nopreroll   | false
     nsfw        | false
     stats       | [0, 3905, 0, 87743]
     still       | https://media.dum...
     stills      | [https://media.du...
     tags        | amerikaan europa ...
     thumbnail   | https://media.dum...
     title       | We staan weer op ...
    only showing top 1 row

    0:00:06.281004



```python
startTime = datetime.datetime.now()

# 1. Tel het totaal aantal videos

display(staticVideosTest.count())

print(datetime.datetime.now()-startTime)
```


    15


    0:00:01.349677



```python
startTime = datetime.datetime.now()

# Register a table
staticVideosTest.cache()
staticVideosTest.createOrReplaceTempView("videos")
table = spark.sql("select count(id), nopreroll from videos group by nopreroll")
table.show()

print(datetime.datetime.now()-startTime)
```

    +---------+---------+
    |count(id)|nopreroll|
    +---------+---------+
    |       15|    false|
    +---------+---------+

    0:00:02.285046


## Batch Processing


```python

# Load videos and comments

startTime = datetime.datetime.now()
staticVideos = spark.read \
    .json("hdfs://namenode:9000/dumpert/videos/") \
    .select(explode("items").alias("videos")) \
    .select( \
      col("videos.date").alias("date") \
    , col("videos.description").alias("description") \
    , col("videos.id").alias("id") \
    , col("videos.media").alias("media") \
    , col("videos.nopreroll").alias("nopreroll") \
    , col("videos.nsfw").alias("nsfw") \
    , col("videos.stats").alias("stats") \
    , col("videos.still").alias("still") \
    , col("videos.stills").alias("stills") \
    , col("videos.tags").alias("tags") \
    , col("videos.thumbnail").alias("thumbnail") \
    , col("videos.title").alias("title")
    )
print("Loading videos took: ")
print(datetime.datetime.now()-startTime)

startTime = datetime.datetime.now()
staticComments = spark.read \
     .json("hdfs://namenode:9000/dumpert/reaguursels/") \
     .select(explode("data.comments").alias("comments")) \
     .select( \
      col("comments.approved").alias("approved") \
    , col("comments.article_id").alias("article_id") \
    , col("comments.article_link").alias("article_link") \
    , col("comments.article_title").alias("article_title") \
    , col("comments.author_is_newbie").alias("author_is_newbie") \
    , col("comments.author_username").alias("author_username") \
    , col("comments.banned").alias("banned") \
    , col("comments.child_comments").alias("child_comments") \
    , col("comments.creation_datetime").alias("creation_datetime") \
    , col("comments.display_content").alias("display_content") \
    , col("comments.html_markup").alias("html_markup") \
    , col("comments.id").alias("id") \
    , col("comments.is_author_premium_visible").alias("is_author_premium_visible") \
    , col("comments.kudos_count").alias("kudos_count") \
    , col("comments.parent_id").alias("parent_id") \
    , col("comments.reference_id").alias("reference_id") \
    , col("comments.report_count").alias("report_count")
    )
print("Loading comments took: ")
print(datetime.datetime.now()-startTime)

# reaguurselsParentDF.printSchema()
# videosParentDF.printSchema()

startTime = datetime.datetime.now()
staticComments.cache()
staticVideos.cache()
print("Caching took: ")
print(datetime.datetime.now()-startTime)

startTime = datetime.datetime.now()
staticComments.show(1, vertical=True)
staticVideos.show(1, vertical=True)
print("Showing data took: ")
print(datetime.datetime.now()-startTime)
```

    Loading videos took:
    0:00:40.153398
    Loading comments took:
    0:04:57.216998
    Caching took:
    0:00:00.076309
    -RECORD 0-----------------------------------------
     approved                  | true
     article_id                | 7109669
     article_link              | https://comments....
     article_title             | Even geduld nog aub
     author_is_newbie          | false
     author_username           | Unloadable
     banned                    | false
     child_comments            | []
     creation_datetime         | 2017-04-20 19:52:45
     display_content           | K*t Ziggo...
     html_markup               | <div class="cmt-c...
     id                        | 239927506
     is_author_premium_visible | false
     kudos_count               | -3
     parent_id                 | 0
     reference_id              | 0
     report_count              | 0
    only showing top 1 row

    -RECORD 0---------------------------
     date        | 2013-10-15 12:26:28
     description | De NS denkt nu ec...
     id          | 6566890_a21801a4
     media       | [[,, FOTO, [[http...
     nopreroll   | false
     nsfw        | false
     stats       | [0, 964, 0, 97493]
     still       | https://media.dum...
     stills      | [https://media.du...
     tags        | trein ns spoor bl...
     thumbnail   | https://media.dum...
     title       | Bladblazers
    only showing top 1 row

    Showing data took:
    0:00:02.557646



```python
startTime = datetime.datetime.now()

# 1. Tel het totaal aantal videos

display(staticVideos.count())
display(staticComments.count())

print(datetime.datetime.now()-startTime)
```


    164354



    7110782


    0:05:20.930826



```python
# Transformations

# Videos
startTime = datetime.datetime.now()

staticVideos.createOrReplaceTempView("videos")
table = spark.sql("SELECT COUNT(id), nopreroll FROM videos GROUP BY nopreroll")
table.show()

print(datetime.datetime.now()-startTime)

# Comments
startTime = datetime.datetime.now()

staticComments.createOrReplaceTempView("comments")
table = spark.sql("SELECT '> 20' as kudos, COUNT(id) FROM comments WHERE kudos_count > 20")
table.show()
table = spark.sql("SELECT '< -20' as kudos, COUNT(id) FROM comments WHERE kudos_count < -20")
table.show()

print(datetime.datetime.now()-startTime)
```

    +---------+---------+
    |count(id)|nopreroll|
    +---------+---------+
    |    28568|     true|
    |   135786|    false|
    +---------+---------+

    0:00:03.342594
    +-----+---------+
    |kudos|count(id)|
    +-----+---------+
    | > 20|  1006972|
    +-----+---------+

    +-----+---------+
    |kudos|count(id)|
    +-----+---------+
    |< -20|   202281|
    +-----+---------+

    0:00:28.307170


## Structured Stream processing (NOT FUNCTIONAL)


```python
# Load comments
## Treat a sequence of files as a stream by picking one file at a time

startTime = datetime.datetime.now()
streamingComments = spark.readStream \
    .schema(staticComments.schema) \ # FIX SCHEMA!
    .option("maxFilesPerTrigger", 1) \
    .json("hdfs://namenode:9000/dumpert/reaguursels/") \
    .select(explode("data.comments").alias("comments")) \
    .select( \
      col("comments.approved").alias("approved") \
    , col("comments.article_id").alias("article_id") \
    , col("comments.article_link").alias("article_link") \
    , col("comments.article_title").alias("article_title") \
    , col("comments.author_is_newbie").alias("author_is_newbie") \
    , col("comments.author_username").alias("author_username") \
    , col("comments.banned").alias("banned") \
    , col("comments.child_comments").alias("child_comments") \
    , col("comments.creation_datetime").alias("creation_datetime") \
    , col("comments.display_content").alias("display_content") \
    , col("comments.html_markup").alias("html_markup") \
    , col("comments.id").alias("id") \
    , col("comments.is_author_premium_visible").alias("is_author_premium_visible") \
    , col("comments.kudos_count").alias("kudos_count") \
    , col("comments.parent_id").alias("parent_id") \
    , col("comments.reference_id").alias("reference_id") \
    , col("comments.report_count").alias("report_count")
    )
print("Loading comments took: ")
print(datetime.datetime.now()-startTime)


# Start counting
startTime = datetime.datetime.now()
streamingCounts = comments.count()
print(datetime.datetime.now()-startTime)

streamingCounts.isStreaming


#
# spark.conf.set("spark.sql.shuffle.partitions", "2")  # keep the size of shuffles small

# query = streamingCountsDF \
#     .writeStream \
#     .format("memory") \       # memory = store in-memory table
#     .queryName("counts") \    # counts = name of the in-memory table
#     .outputMode("complete") \ # complete = all the counts should be in the table
#     .start()
```

Work in progress
