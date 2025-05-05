from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import json

with open('/opt/airflow/config/config.json') as f:
    config = json.load(f)

airports_input_path = config["local"]['airports_input_path']
bookings_input_path = config["local"]['bookings_input_path']
target_base_path = config["local"]['target_base_path']
archive_path = config["local"]['archive_path']

start_date = config["start_date"]
end_date = config["end_date"]

with DAG(
    dag_id='load_local_files',
    description='Load files from local file system.',
) as dag:
    
    load_airports = SparkSubmitOperator(
        task_id='load_airports',
        application='/scripts/load_airports.py',
        conn_id='spark_default',
        application_args=[airports_input_path, bookings_input_path, target_base_path, archive_path],
        verbose=True,
        conf={
            "spark.jars.packages": "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.9.0"
        }
    )
    
    extract_bookings = SparkSubmitOperator(
        task_id='extract_bookings',
        application='/scripts/extract_bookings.py',
        conn_id='spark_default',
        application_args=[airports_input_path, bookings_input_path, target_base_path, archive_path],
        verbose=True
    )
    
    load_flights = SparkSubmitOperator(
        task_id='load_flights',
        application='/scripts/load_flights.py',
        conn_id='spark_default',
        application_args=[airports_input_path, bookings_input_path, target_base_path],
        verbose=True,
        conf={
            "spark.jars.packages": "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.9.0"
        }
    )
    
    load_passengers = SparkSubmitOperator(
        task_id='load_passengers',
        application='/scripts/load_passengers.py',
        conn_id='spark_default',
        application_args=[airports_input_path, bookings_input_path, target_base_path],
        verbose=True,
        conf={
            "spark.jars.packages": "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.9.0"
        }
    )
    
    load_flights_passengers_asn = SparkSubmitOperator(
        task_id='load_flights_passengers_asn',
        application='/scripts/load_flights_passengers_asn.py',
        conn_id='spark_default',
        application_args=[airports_input_path, bookings_input_path, target_base_path],
        verbose=True,
        conf={
            "spark.jars.packages": "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.9.0"
        }
    )
    
    create_report = SparkSubmitOperator(
        task_id='create_report',
        application='/scripts/create_report.py',
        conn_id='spark_default',
        application_args=[airports_input_path, bookings_input_path, target_base_path, start_date, end_date],
        verbose=True,
        conf={
            "spark.jars.packages": "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.9.0"
        }
    )

    load_airports >> extract_bookings >> [load_flights, load_passengers, load_flights_passengers_asn] >> create_report