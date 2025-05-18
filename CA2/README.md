[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/ArshiAbolghasemi/DataScience-UT-Projects)
# Darooghe - Payment Processing Pipeline

Darooghe is a sophisticated payment processing and analysis system designed to handle transaction data flows, detect fraud, analyze commission structures, and provide insights into payment patterns.

## Overview

The project implements a payment processing pipeline with real-time fraud detection and batch analytics capabilities. It uses a domain-driven design approach with clear separation of concerns across domain, application, and infrastructure layers.

## Features

- **Transaction Processing**: Process and validate payment transactions
- **Fraud Detection**: Real-time fraud detection through streaming data analysis
- **Commission Analysis**: Batch analysis of commission data to optimize pricing models
- **Data Validation**: Comprehensive validation of transaction data

## Technology Stack

- **Python 3.12**: Modern Python features
- **Apache Spark**: Distributed data processing for batch and stream processing
- **Apache Kafka**: Message broker for streaming data
- **MongoDB**: Document database for storing transaction data and analysis results
- **Prometheus & Grafana**: Monitoring and visualization

## Project Structure

The project follows a domain-driven design with three main layers:

```
darooghe/  
├── application/          # Application services, repositories, and jobs  
│   ├── job/              # Batch and stream processing jobs  
│   ├── repository/       # Data access  
│   └── service/          # Business services  
├── domain/               # Core business logic  
│   ├── entity/           # Business entities  
│   ├── factory/          # Entity factories  
│   └── util/             # Shared utilities  
└── infrastructure/       # Technical implementation details  
    ├── data_processing/  # Spark configuration and processing  
    ├── messaging/        # Kafka integration  
    └── persistence/      # MongoDB integration  
```

## Key Components

### Domain Entities
- **Transaction**: Core entity representing payment transactions with validation logic
- **Commission Models**: Various commission structures (flat, tiered, volume-based)

### Batch Jobs
- **Commission Analysis**: Analyzes commission patterns and simulates different commission models

### Stream Processing
- **Fraud Detection**: Real-time detection of fraudulent transactions

## Setup and Deployment

### Prerequisites
- Docker and Docker Compose
- Python 3.12+ (for local development)

### Environment Setup
Create a `.env` file in the project root with the following configuration:

```
# kafka
KAFKA_CLUSTER_ID='MkU3OEVBNTcwNTJENDM2Qk'

# mongodb
MONGO_INITDB_ROOT_USERNAME=root
MONGO_INITDB_ROOT_PASSWORD=example
MONGO_URI=mongodb://root:example@mongodb:27017/

# grafana
GF_SECURITY_ADMIN_USER=admin
GF_SECURITY_ADMIN_PASSWORD=admin
```

### Docker Deployment
The project uses Docker Compose for easy deployment of all services:

```bash
# Build and start all services
docker-compose build
docker-compose up -d
```

## Infrastructure Components

- **Kafka**: Message broker for transaction streaming
- **MongoDB**: Persistent storage for transactions and analysis results
- **Spark**: Distributed data processing engine
- **Prometheus & Grafana**: Monitoring and visualization

## Development

### Requirements
- Python 3.12+
- Docker and Docker Compose
