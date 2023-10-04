#!/bin/bash

# Function to stop a service by port
stop_service() {
    local port="$1"

    local pid=$(lsof -ti :$port)
    if [ -z "$pid" ]; then
        echo "No service found on port $port."
    else
        echo "Stopping service on port $port..."
        kill -9 "$pid"
        echo "Service stopped!"
    fi
}

# Stop metaServer
stop_service 8000
stop_service 8001

# Stop dataServer
stop_service 9000
stop_service 9001
stop_service 9002
stop_service 9003
