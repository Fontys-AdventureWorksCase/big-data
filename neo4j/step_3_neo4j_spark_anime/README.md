# Neo4j Connector for Apache Spark Zeppelin Notebooks

Collection of notebooks to get started with Neo4j Connector for Apache Spark

```
docker-compose up
```

## Zeppelin Notebook

Visit [localhost:8080](http://localhost:8080)

## Neo4j Instance

By default, the username/password is `neo4j/password`

Visit [localhost:7474](http://localhost:7474)

## Import anime

Make sure these files are in the directory neo4j/import

- users_cleaned.csv
- anime_cleaned.csv
- animelists_cleaned.csv

## Import using admin-tool

NB: I didn't actually get this to work. These are the steps I took.

`docker-compose exec neo4j bash`

Make sure the anime db is empty
`rm -r /data/*/anime/`

Change to the anime db in the configuration (in neo4j community edition only 1 database can be accessed at a time)

`nano /conf/neo4j.conf` (or `nano neo4j/conf/neo4j.conf` outside the container)

![image](https://user-images.githubusercontent.com/4932561/120935908-3ef5eb80-c705-11eb-9ed1-1a0dc0c10568.png)

Header rows should be changed to:
- _anime_cleaned.csv_
anime_id:ID(Anime),title,title_english,title_japanese,title_synonyms,image_url,type,source,episodes:int,status,airing,aired_string,aired,duration,rating,score,scored_b...

- _users_cleaned.csv_
username:ID(User),user_id:int,user_watching:int,user_completed:int,user_onhold:int,user_dropped:int,user_plantowatch:int,user_days_spent_watching:double,gender,location,birth_date,access_rank:int,join_date,last_online,stats_mean_score:float,stats_rewatched:float,stats_episodes:int

- _animelists_cleaned.csv_
username:START_ID,anime_id:END_ID,my_watched_episodes:int,my_start_date,my_finish_date,my_score:int,my_status:int,my_rewatching,my_rewatching_ep:int,my_last_updated,my...

```
neo4j-admin import --database=anime \
      --nodes=Anime=/import/anime/anime_cleaned.csv \
      --nodes=User=/import/anime/users_cleaned.csv \
      --relationships=WATCHED=/import/anime/animelists_cleaned.csv \
      --trim-strings=true
```

Import failed on type constraints due to inconsistent data. Also, to use the DateTime type, the space in DateTime columns should be transformed to a T.

After this, I started playing with PySpark and Zeppelin.


## Import with PySpark

![import-anime-Zeppelin](https://user-images.githubusercontent.com/4932561/120935497-65b32280-c703-11eb-825d-a6f4a2ce75ed.png)

![image](https://user-images.githubusercontent.com/4932561/120935411-ede4f800-c702-11eb-8ba6-1e1f50686785.png)

Small batches help prevent deadlocks. Default is 5000. Smaller is recommended, in part because a relationship import also locks both nodes. 

![image](https://user-images.githubusercontent.com/4932561/120935416-f1787f00-c702-11eb-9d0a-f189449760e2.png)

This creates an index on nodes before nodes are created. It makes the node lookup faster on importing relationships.

APOC lib/plugin can be used (on top of Cypher) for optimization and added functionality. Spark already uses some of its functions under water.

Import successful:

![image](https://user-images.githubusercontent.com/4932561/120935427-f76e6000-c702-11eb-958f-63d7a4354587.png)

Import the .json into Zeppelin to see the full code.

## Considerations

The admin-tool import is still preferred because it should have better optimization and is supposed to be faster. Perhaps it would work with --skip-bad-relationships and --skip-duplicate-nodes, and checking import reports could help.

If you get it to work, don't forget to create indexes.




