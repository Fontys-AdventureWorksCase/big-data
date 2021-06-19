
## Files
hadoop /breweries/breweries.csv \
hadoop /ml_data (movies) \
hadoop /dumpert

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


## Structured Stream processing


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


    ---------------------------------------------------------------------------

    AnalysisException                         Traceback (most recent call last)

    <ipython-input-8-f80a5eaf039e> in <module>
          7     .option("maxFilesPerTrigger", 1) \
          8     .json("hdfs://namenode:9000/dumpert/reaguursels/") \
    ----> 9     .select(explode("data.comments").alias("comments")) \
         10     .select( \
         11       col("comments.approved").alias("approved") \


    /usr/local/lib/python3.7/dist-packages/pyspark/sql/dataframe.py in select(self, *cols)
       1419         [Row(name=u'Alice', age=12), Row(name=u'Bob', age=15)]
       1420         """
    -> 1421         jdf = self._jdf.select(self._jcols(*cols))
       1422         return DataFrame(jdf, self.sql_ctx)
       1423 


    /usr/local/lib/python3.7/dist-packages/py4j/java_gateway.py in __call__(self, *args)
       1303         answer = self.gateway_client.send_command(command)
       1304         return_value = get_return_value(
    -> 1305             answer, self.gateway_client, self.target_id, self.name)
       1306 
       1307         for temp_arg in temp_args:


    /usr/local/lib/python3.7/dist-packages/pyspark/sql/utils.py in deco(*a, **kw)
        135                 # Hide where the exception came from that shows a non-Pythonic
        136                 # JVM exception message.
    --> 137                 raise_from(converted)
        138             else:
        139                 raise


    /usr/local/lib/python3.7/dist-packages/pyspark/sql/utils.py in raise_from(e)


    AnalysisException: cannot resolve '`data.comments`' given input columns: [approved, article_id, article_link, article_title, author_is_newbie, author_username, banned, child_comments, creation_datetime, display_content, html_markup, id, is_author_premium_visible, kudos_count, parent_id, reference_id, report_count];;
    'Project [explode('data.comments) AS comments#2463]
    +- StreamingRelation DataSource(org.apache.spark.sql.SparkSession@39964bc3,json,List(),Some(StructType(StructField(approved,BooleanType,true), StructField(article_id,LongType,true), StructField(article_link,StringType,true), StructField(article_title,StringType,true), StructField(author_is_newbie,BooleanType,true), StructField(author_username,StringType,true), StructField(banned,BooleanType,true), StructField(child_comments,ArrayType(StructType(StructField(approved,BooleanType,true), StructField(article_id,LongType,true), StructField(article_link,StringType,true), StructField(article_title,StringType,true), StructField(author_is_newbie,BooleanType,true), StructField(author_username,StringType,true), StructField(banned,BooleanType,true), StructField(creation_datetime,TimestampType,true), StructField(display_content,StringType,true), StructField(html_markup,StringType,true), StructField(id,LongType,true), StructField(is_author_premium_visible,BooleanType,true), StructField(kudos_count,LongType,true), StructField(parent_id,LongType,true), StructField(reference_id,LongType,true), StructField(report_count,LongType,true)),true),true), StructField(creation_datetime,TimestampType,true), StructField(display_content,StringType,true), StructField(html_markup,StringType,true), StructField(id,LongType,true), StructField(is_author_premium_visible,BooleanType,true), StructField(kudos_count,LongType,true), StructField(parent_id,LongType,true), StructField(reference_id,LongType,true), StructField(report_count,LongType,true))),List(),None,Map(maxFilesPerTrigger -> 1, path -> hdfs://namenode:9000/dumpert/reaguursels/),None), FileSource[hdfs://namenode:9000/dumpert/reaguursels/], [approved#2429, article_id#2430L, article_link#2431, article_title#2432, author_is_newbie#2433, author_username#2434, banned#2435, child_comments#2436, creation_datetime#2437, display_content#2438, html_markup#2439, id#2440L, is_author_premium_visible#2441, kudos_count#2442L, parent_id#2443L, reference_id#2444L, report_count#2445L]


import spark.implicits._            

    
fileStream = spark.readStream \
                .format("")
    
 val file = spark.readStream.schema(schemaforfile).csv("C:\\SparkScala\\fakefriends.csv")  

 file.writeStream.format("parquet").start("C:\\Users\\roswal01\\Desktop\\streamed") 
 
 spark.stop()

val file = spark.readStream.schema(schemaforfile).csv("C:\\SparkScala\\fakefriends.csv")  

val query = file.writeStream.format("parquet")
    .option("checkpointLocation", "path/to/HDFS/dir")
    .start("C:\\Users\\roswal01\\Desktop\\streamed") 

query.awaitTermination()

df = spark.readStream \
    .format("rate") \
    .option("rowsPerSecond", 1) \
    .load()

# Write the streaming DataFrame to a table
df.writeStream \
    .option("checkpointLocation", "path/to/checkpoint/dir") \
    .toTable("myTable")

# Check the table result
spark.read.table("myTable").show()

# Transform the source dataset and write to a new table
spark.readStream \
    .table("myTable") \
    .select("value") \
    .writeStream \
    .option("checkpointLocation", "path/to/checkpoint/dir") \
    .format("parquet") \
    .toTable("newTable")

# Check the new table result
spark.read.table("newTable").show()

```python

```


```python

```
