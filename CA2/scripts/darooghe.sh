#!/bin/bash

set -e

LOG_DIR="/app/logs"
LOG_FILE="${LOG_DIR}/darooghe.log"

mkdir -p "${LOG_DIR}"

echo "======== Starting Darooghe Pipeline Application ========"
echo "$(date) - Starting application" >> "${LOG_FILE}"

run_command() {
    local cmd="$1"
    local name="$2"
    local background="${3:-false}"

    echo "$(date) - Starting $name..." >> "${LOG_FILE}"
    echo "Starting $name..."
    if [ "$background" = true ]; then
        nohup $cmd >> "${LOG_FILE}" 2>&1 &
        echo "$(date) - $name started in background" >> "${LOG_FILE}"
    else
        $cmd >> "${LOG_FILE}" 2>&1
        echo "$(date) - $name completed" >> "${LOG_FILE}"
    fi
}

run_command \
    "python3 -m darooghe.infrastructure.persistence.mongo_setup" \
    "MongoDB Setup"

run_command \
    "python3 -m darooghe.application.service.transaction_data_loader" \
    "Transaction Data Loader"

run_command \
    "python3 -m darooghe.application.service.transaction_producer" \
    "Transaction Producer" \
    true

run_command \
    "python3 -m darooghe.application.service.transaction_consumer" \
    "Transaction Consumer" \
    true

run_command \
    "python3 -m darooghe.application.job.batch.transaction_pattern_job" \
    "Transaction Pattern Job"

run_command \
    "python3 -m darooghe.application.job.batch.commission_analysis_job" \
    "Commission Batch Analysis Job"

run_command \
    "python3 -m darooghe.application.job.stream.fraud_detection_job" \
    "Fraud Detection Job" \
    true

run_command \
    "docker exec -it darooghe-app python -m darooghe.application.job.stream.commission_analytics_job" \
    "Commission Stream Analytics Job" \
    true

echo "$(date) - All components started" >> "${LOG_FILE}"
echo "All components started successfully"

