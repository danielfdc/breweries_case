#!/bin/sh
set -euo pipefail

echo "Executing setup script..."
python3 ./create_tables_script.py

RC=$0

if [ $RC -ne 0 ]; then
    echo "Error executing create_tables_script.py"
    exit $RC
fi

# Navigate to the directory containing docker-compose.yml
echo "Navigating to docker directory..."
cd ..

# Build Docker images
echo "Building Docker images..."
docker compose build

if [ $RC -ne 0 ]; then
    echo "Error executing create_tables_script.py"
    exit $RC
fi

# Start Docker containers in detached mode
echo "Starting Docker containers..."
docker compose up -d

if [ $RC -ne 0 ]; then
    echo "Error executing create_tables_script.py"
    exit $RC
fi

echo "Setup script completed successfully!"
exit 0