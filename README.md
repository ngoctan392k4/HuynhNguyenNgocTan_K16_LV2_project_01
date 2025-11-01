# KAFKA TO MONGODB DATA PIPELINE

# Table of Contents
- [KAFKA TO MONGODB DATA PIPELINE](#kafka-to-mongodb-data-pipeline)
- [Table of Contents](#table-of-contents)
  - [❓ Description](#-description)
  - [🎯 Objective](#-objective)
  - [🛠 Tech Stack](#-tech-stack)
  - [📂 Repository Structure](#-repository-structure)
  - [⚙️ Setup and Configuration Environment](#️-setup-and-configuration-environment)
  - [📝 How to Run](#-how-to-run)
  - [🚀 Future Enhancements](#-future-enhancements)

## ❓ Description
This project is a simple data pipeline used to read data from a source Apache Kafka topic, transfers it to a local Kafka topic before inserting all messages into a MongoDB database

## 🎯 Objective
Based on the given source kafka topic, design two main flow including:
- Producer: Consume data from the provided source Kafka topic and produces it to a local Kafka topic that is already set up
- Consumer: Consume data from the local Kafka topic, which is created, and stores it into a MongoDB collection

## 🛠 Tech Stack
- Python
- Apache Kafka: Using kafka-python library
- MongoDB: Using the pymongo library

## 📂 Repository Structure
- environment
  - config.yml
- logs
  - kafka_log.log
- src
  - kafka_to_kafka
  - kafka_to_mongodb
- .gitignore
- README.md
- requirements.txt

## ⚙️ Setup and Configuration Environment
- Create the ```config.yml``` file in the ```environment``` folder from the root's directory to store connection credentials
      **In this project both have the same topic name and kafka configuration**
    - MongoDB
      - MONGODB_URL: the mongodb url
    - Kafka bootstrap server
      - LOCAL_BOOTSTRAP_SERVERS: the local bootstrap server kafka
      - SOURCE_BOOTSTRAP_SERVERS: the source bootstrap server kafka
      - KAFKA_TOPIC: the topic name
    - Kafka Configuration
      - SECURITY_PROTOCOL: SASL_SSL
      - SASL_MECHANISM: PLAIN
      - SASL_PLAIN_USERNAME: kafka username
      - SASL_PLAIN_PASSWORD: kafka passwor
    - LOGGING Configuration
      - LOGGING
          - level: INFO
          - log_file: logs/kafka_log.log
          - to_console: true

## 📝 How to Run
1. Create virtual environment and install all required dependencies in the ```requirements.txt``` file
2. Run the package kafka_to_kafka
3. Run the package kafka_to_mongodb

## 🚀 Future Enhancements
- Manual offset management: Implement manual offset management instead of depending on auto-commit-offsets of Apache Kafka in order to ensure that offsets are only committed after the data has been successfully written to local Kafka in the package kafka_to_kafka and to MongoDB in the package kafka_to_mongodb. As a result, this enhancement has ability to prevent data loss if programs fails after committing the offset but before writing to the database