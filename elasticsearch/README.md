# Elastic Search
Elasticsearch is a search engine based on the Lucene library. It provides a distributed, multitenant-capable full-text search engine with an HTTP web interface and schema-free JSON documents.

## Getting started
To get started, simply runn the below Docker compose command;
```
docker-compose up
```

The following endpoints are available from the host;

| Service               | Endpoint              | Description                                       |
|-----------------------|-----------------------|---------------------------------------------------|
| Kibana                | http://localhost:8080 | Backoffice UI to interact with Elastic Search     |
| Elastic Search master | http://localhost:9200 | Exposed HTTP API from the master node             |
| Elastic Search data   | http://localhost:9201 | Exposed HTTP API from the data node               |
| Elastic Search client | http://localhost:9202 | Exposed HTTP API from the client/ingest node      |
