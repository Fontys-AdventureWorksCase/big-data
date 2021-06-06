#! /usr/bin/env bash

echo "Starting docker ."
docker-compose up -d --build

function clean_up {
    echo "\n\nShutting down....\n\n"
    
    docker-compose down -v
}

trap clean_up EXIT


docker exec -it jupyterlab  /opt/conda/bin/jupyter server list

echo '''
Spark Master - http://localhost:8080
Spark Worker 1
Spark Worker 2

==============================================================================================================

Use <ctrl>-c to quit'''

read -r -d '' _ </dev/tty
echo '\n\nTearing down the Docker environment, please wait.\n\n'

docker-compose down  -v