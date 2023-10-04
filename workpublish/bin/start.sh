#!/bin/bash

metaServer="../metaServer/metaServer.jar"
dataServer="../dataServer/dataServer.jar"

# Function to start a service
start_service() {
    local service="$1"
    local port="$2"

    if [ -f "$service" ]; then
        echo "Starting $service on port $port..."
        java -jar "$service" --server.port="$port" &
        echo "Service started!"
    else
        echo "Error: $service not found."
    fi
}

# Start metaServer
start_service "$metaServer" 8000
start_service "$metaServer" 8001

# Start dataServer
start_service "$dataServer" 9000
start_service "$dataServer" 9001
start_service "$dataServer" 9002
start_service "$dataServer" 9003
