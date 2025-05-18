#!/bin/bash
PROJECT_ROOT=$(cd "$(dirname "$0")/.."; pwd)
IMAGE_NAME="jupyter/scipy-notebook"
CONTAINER_NAME="ut-ds-ca2-jupyter-notebook"
PORT=8888

open_browser() {
    local url=$1
    echo "Opening Jupyter notebook in browser: $url"

    case "$(uname -s)" in
        Linux*)
            if command -v xdg-open > /dev/null; then
                xdg-open "$url" &
            elif command -v gnome-open > /dev/null; then
                gnome-open "$url" &
            else
                echo "Could not detect a browser opener. Please visit the URL manually."
            fi
            ;;
        Darwin*)
            open "$url" &
            ;;
        *)
            echo "Unsupported OS detected. Please visit the URL manually."
            ;;
    esac
}

get_jupyter_token() {
    sleep 3

    local server_output=$(docker exec -it "$CONTAINER_NAME" jupyter server list 2>/dev/null)

    if [ $? -eq 0 ] && [ -n "$server_output" ]; then

        local token=$(echo "$server_output" | sed -n 's/.*token=\([a-zA-Z0-9]*\).*/\1/p' | head -1)

        if [ -n "$token" ]; then
            echo "$token"
            return
        fi
    fi
}


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


TOKEN=$(get_jupyter_token)

if [ -n "$TOKEN" ]; then
    JUPYTER_URL="http://localhost:$PORT/lab?token=$TOKEN"
    echo -e "\nJupyter notebook URL with token: $JUPYTER_URL"
    open_browser "$JUPYTER_URL"
else
    JUPYTER_URL="http://localhost:$PORT"
    echo -e "\nCould not retrieve token. Opening Jupyter at: $JUPYTER_URL"
    echo "You may need to enter a token or password on the login page."
    open_browser "$JUPYTER_URL"
fi

echo -e "\nAccess URL: http://localhost:$PORT"
