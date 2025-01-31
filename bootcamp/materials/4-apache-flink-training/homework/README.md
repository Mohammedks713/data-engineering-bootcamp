# Flink Job for Sessionizing Web Events

## Overview

This Flink job ingests web traffic data from a Kafka topic, performs sessionization using a 5-minute gap, and stores the aggregated session data in a PostgreSQL database.

## Setup

### Prerequisites

1. Kafka Broker running and accessible via `KAFKA_URL`.
2. PostgreSQL database running and accessible via `POSTGRES_URL`.
3. Set up environment variables for Kafka and PostgreSQL authentication as described in the `TESTING.txt`.

### Running the Job

To run the Flink job, use the following command:

```bash
python flink-homework.py