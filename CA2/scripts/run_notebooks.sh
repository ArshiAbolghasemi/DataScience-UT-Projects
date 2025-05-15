#!/bin/bash
PROJECT_ROOT=$(cd "$(dirname "$0")/.."; pwd)

IMAGE_NAME="jupyter/scipy-notebook"
CONTAINER_NAME="ut-ds-ca2-jupyter-notebook"
PORT=8888

if docker container inspect "$CONTAINER_NAME" >/dev/null 2>&1; then
  if [ "$(docker inspect -f '{{.State.Running}}' "$CONTAINER_NAME")" == "true" ]; then
    echo "Container '$CONTAINER_NAME' is already running."
  else
    echo "Starting existing container '$CONTAINER_NAME'..."
    docker start "$CONTAINER_NAME"
  fi
else
  echo "Creating and starting new container '$CONTAINER_NAME'..."
  docker run -d \
    -p $PORT:8888 \
    -v "$PROJECT_ROOT/notebooks:/home/jovyan/work" \
    -v "$PROJECT_ROOT/logs:/var/log/jupyter" \
    --network darooghe-network \
    --env-file "$PROJECT_ROOT/.env" \
    --name $CONTAINER_NAME \
    $IMAGE_NAME
fi

echo -e "\nAccess at: http://localhost:$PORT"
