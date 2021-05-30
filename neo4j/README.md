# neo4j

## Run

`docker-compose up -d`

## Access

Open browser, navigate to localhost:7474 and then set Connect URL to localhost:7687.

Or access a running dev example on esx.rdjsoft.nl:7474 (browser) and esx.rdjsoft.nl:7687 (dbms).

## Authentication

Username: neo4j
Password: warriors

## Import

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
