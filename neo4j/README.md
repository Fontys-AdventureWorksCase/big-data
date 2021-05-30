# neo4j

## Run

`docker-compose up -d`

## Access

Open browser, navigate to localhost:7474 and then set Connect URL to localhost:7687.

Or access a running dev example on esx.rdjsoft.nl:7474 (browser) and esx.rdjsoft.nl:7687 (dbms).

## Authentication

Username: neo4j
Password: warriors

NB: If the password is not accepted, try 'neo4j' as a password and when asked to change it type 'warriors'

## Import csv using the neo4j browser

Access the neo4j browser, authenticate, and then type the following:

```
CREATE CONSTRAINT UniqueCharacterId ON (c:Character) ASSERT c.id IS UNIQUE

:auto USING PERIODIC COMMIT 500
LOAD CSV WITH HEADERS FROM "file:///got-s1-nodes.csv" AS row
MERGE (c:Character {id:row.Id})
SET c.name = row.Label

:auto USING PERIODIC COMMIT 500
LOAD CSV WITH HEADERS FROM "file:///got-s1-edges.csv" AS row
MATCH (source:Character {id: row.Source})
MATCH (target:Character {id: row.Target})
MERGE (source)-[:SEASON1 {weight: toInteger(row.Weight)}]-(target)
```
There are files available for s1 throught s8, both nodes and edges.

Type the following to view the data
`match (n) return n limit 50`

## Import csv using neo4j-admin tool

```
local:~/big-data/neo4j$           docker-compose exec neo4j bash
neo4j-container:/var/lib/neo4j#   bin/neo4j-admin import --database=movies \
                                  --nodes=/import/movies.csv \
                                  --nodes=/import/actors.csv \
                                  --relationships=/import/roles.csv
```

```
Neo4j version: 4.2.7
Importing the contents of these files into /data/databases/movies:
Nodes:
  /import/movies.csv
  /import/actors.csv

Relationships:
  /import/roles.csv

[…]

IMPORT DONE in 682ms.
Imported:
  6 nodes
  9 relationships
  24 properties
Peak memory usage: 1.004GiB
```

NB: The neo4j community edition only allows access to 1 database. You need to set in the config file which database you want to use before you start the container.

`local:~/big-data/neo4j$ nano volumes/conf/neo4j.conf`

and then add the line `dbms.active_database=movies`

![image](https://user-images.githubusercontent.com/4932561/120121267-bd510b80-c1a2-11eb-8337-fc3aeb2d40ae.png)

Make sure to restart the container afterwards
`docker-compose down`
`docker-compose up -d`

