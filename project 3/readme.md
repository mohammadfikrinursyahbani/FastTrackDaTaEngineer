# ETL Pipeline

![Pipeline](Image%20Pipeline.png)

## Prerequisites

1. **Setup Airflow**
    Ikuti langkah-langkahnya [airflow_setup](./airflow_setup)

2. **Install Library**
    ```bash
    pip install -r requirements.txt
    ```

## Getting Started

1. **Clone the repository**
    ```bash
    git clone <repository-url>
    cd <repository-directory>
    ```

2. **Setup Virtual Environment**
    ```bash
    python -m venv venv
    source venv/bin/activate  # On Windows use `venv\Scripts\activate`
    ```

3. **Install Dependencies**
    ```bash
    pip install -r requirements.txt
    ```

## Usage

1. **Trigger the DAG**
    - Open Airflow web UI at `http://localhost:8080`
    - Trigger the ETL pipeline DAG

2. **Monitor the Pipeline**
    - Use the Airflow web UI to monitor the progress and logs of the ETL pipeline