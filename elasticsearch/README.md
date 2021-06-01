# Elastic Search
Elasticsearch is a search engine based on the Lucene library. It provides a distributed, multitenant-capable full-text search engine with an HTTP web interface and schema-free JSON documents.

## Getting started
To get started, simply runn the below Docker compose command;
```
docker-compose up
```

The following endpoints are available from the host;

| Service                     | Endpoint              | Description                                       |
|-----------------------------|-----------------------|---------------------------------------------------|
| Kibana                      | http://localhost:8080 | Backoffice UI to interact with Elastic Search     |
| Elastic Search master       | http://localhost:9200 | Exposed HTTP API from the master node             |
| Elastic Search data         | http://localhost:9201 | Exposed HTTP API from the data node               |
| Elastic Search data replica | http://localhost:9202 | Exposed HTTP API from the data replica node       |
| Elastic Search client       | http://localhost:9203 | Exposed HTTP API from the client/ingest node      |

## Importing data
To make the best use of Elastic Search, try importing a very large dataset. If you a massive JSON file that cannot be imported in one go, we can use ElasticDump to handle the split and bulk for us.

Install ElasticDump using NPM:
```
npm i elasticdump -g
```

Then import our massive JSON:
```
elasticdump \
    --input="./users_filtered.json" \
    --output="http://localhost:9200" \
    --output-index="users_filtered" \
    --limit=100000 \
    --type=data \
    --transform="doc._source=Object.assign({},doc)"
```

Do note that bulk importing JSON files requires the JSON to have a specific format. See ElasticSearch Bulk API documentation.