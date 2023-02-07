import os
import logging

# Importing the DAG class from the airflow package.
from airflow import DAG
# A function that returns the date of the day before the current date.
from airflow.utils.dates import days_ago

# Functions to handle bigquery
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator, BigQueryInsertJobOperator

# Functions to handle dataproc
from airflow.providers.google.cloud.operators.dataproc import ClusterGenerator, DataprocCreateClusterOperator, \
                                                                DataprocDeleteClusterOperator, DataprocSubmitJobOperator
# Our airflow operators
from airflow.utils.task_group import TaskGroup
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

# Helps us to interact with GCP Storage
from google.cloud import storage

# To allow us to interact with BigQuery
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator

# Take environmental variables into local variables
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
REGION = "asia-southeast2"
ZONE = "asia-southeast2-a"

path = "gs://goog-dataproc-initialization-actions-asia-east2/python/pip-install.sh"
# path = f"gs://goog-dataproc-initialization-{REGION}/python/pip-install.sh"
path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
FILES= {'covid_cases': 'WHO-COVID-19-global-data', 'vaccination': 'who-data/vaccination-data'}
FILES_LIST = list(FILES.keys())

DATASET = "covid19_data"
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", "covid19_data_all")
PYSPARK_URI_LOC = f'gs://{BUCKET}/dataproc/data_processing.py'


CLUSTER_GENERATOR_CONFIG = ClusterGenerator(
    project_id=PROJECT_ID,
    zone=ZONE,
    master_machine_type="e2-standard-4",
    master_disk_size=500,
    num_masters=1,
    num_workers=2,                         
    idle_delete_ttl=900,                    # idle time before deleting cluster
    init_actions_uris=[path],
    metadata={'PIP_PACKAGES': 'spark-nlp pyyaml requests pandas openpyxl'},
).make()



def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround

    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)


default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="data_ingestion_gcs_dag",
    schedule_interval="@once",
    default_args=default_args,
    catchup=False,
    max_active_runs=3,
    tags=['covid19_task'],
) as dag:
    with TaskGroup(group_id='download_local_and_upload_GCS') as ingest_to_gcs:
        for FTYPE, FNAME in FILES.items():

            download_dataset_task = BashOperator(
                task_id=f"download_{FTYPE}_dataset_task",
                bash_command=f"curl -sS https://covid19.who.int/{FNAME}.csv > {path_to_local_home}/{FTYPE}.csv"
            )


            local_to_gcs_task = PythonOperator(
                task_id=f"{FTYPE}_local_to_gcs_task",
                python_callable=upload_to_gcs,
                op_kwargs={
                    "bucket": BUCKET,
                    "object_name": f"raw/{FTYPE}/{FTYPE}.csv",
                    "local_file": f"{path_to_local_home}/{FTYPE}.csv",
                },
            )

            delete_dataset_task = BashOperator(
                task_id=f"delete_{FTYPE}_dataset_task",
                bash_command=f"rm {path_to_local_home}/{FTYPE}.csv"
            )


            download_dataset_task >> local_to_gcs_task >> delete_dataset_task
        

            
            
    with TaskGroup(group_id='process_spark') as process_spark:

        create_cluster_operator_task = DataprocCreateClusterOperator(
            task_id='create_dataproc_cluster',
            cluster_name="covid19-process-cluster",
            project_id=PROJECT_ID,
            region=REGION,
            cluster_config=CLUSTER_GENERATOR_CONFIG
        )

        pyspark_job = {
                "reference": {"project_id": PROJECT_ID},
                "placement": {"cluster_name": 'covid19-process-cluster'},
                "pyspark_job": {
                    "main_python_file_uri": PYSPARK_URI_LOC,
                    "jar_file_uris": ["gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar"],
                    "args": [
                        f"--input_covid_data=gs://{BUCKET}/raw/{FILES_LIST[0]}/{FILES_LIST[0]}.csv",
                        f"--input_vaccination_data=gs://{BUCKET}/raw/{FILES_LIST[1]}/{FILES_LIST[1]}.csv",
                        f"--output_covid_data=gs://{BUCKET}/clean/{FILES_LIST[0]}",
                        f"--output_vaccination_data=gs://{BUCKET}/clean/{FILES_LIST[1]}"
                        ]
                }
            }

        pyspark_task = DataprocSubmitJobOperator(
                task_id='spark_transform_sparksubmit',
                job=pyspark_job,
                region=REGION,
                project_id=PROJECT_ID,
                trigger_rule='all_done'
        )
        
        delete_cluster = DataprocDeleteClusterOperator(
            task_id="delete_cluster", 
            project_id=PROJECT_ID,
            cluster_name="covid19-process-cluster", 
            region=REGION
        )

        create_cluster_operator_task >> pyspark_task >> delete_cluster

    with TaskGroup(group_id='gcp_to_bigquery') as gcp_to_bigquery:
        for FTYPE, FNAME in FILES.items():

            bigquery_external_table_task = BigQueryCreateExternalTableOperator(
                task_id=f'bq_{FTYPE}_{DATASET}_external_table_task',
                table_resource={
                    "tableReference": {
                        "projectId": PROJECT_ID,
                        "datasetId": BIGQUERY_DATASET,
                        "tableId": f"{FTYPE}_{DATASET}_external_table",
                        },

                    "externalDataConfiguration": {
                        "sourceFormat": "PARQUET",
                        "sourceUris": [f"gs://{BUCKET}/clean/{FTYPE}/*.parquet"],
                    },
                },
            )

            bigquery_external_table_task
    
    ingest_to_gcs >> process_spark >> gcp_to_bigquery
