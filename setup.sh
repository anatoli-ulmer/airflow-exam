#!/bin/bash

# Prompt for OpenWeatherMap API key
read -p "Please enter your OpenWeatherMap API key: " API_KEY

# Define the .env file content
cat > .env <<EOF
AIRFLOW_UID=1000
AIRFLOW_GID=0
PROJECT_DIR=/home/ubuntu/exam_airflow/exam_ULMER
OPENWEATHERMAP_API_KEY=${API_KEY}
EOF

echo ".env file created with your OpenWeatherMap API key."