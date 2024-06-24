# Airflow in Docker

## Prerequisites

Before running the project, ensure you have Docker Desktop installed. You can find the installation instructions [here](https://docs.docker.com/engine/install/).

## Getting Started

To run the project:

1. Navigate to the directory containing the `docker-compose.yaml` file.

2. Initialize the database:
    ```sh
    docker compose up airflow-init
    ```

3. Start Airflow:
    ```sh
    docker compose up --build
    ```
    This will automatically install the required packages.


4. Open your web browser and navigate to [http://localhost:8080](http://localhost:8080).

5. Log in with the following credentials:
    - **Username:** `airflow`
    - **Password:** `airflow`



