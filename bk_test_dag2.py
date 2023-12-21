"""
This is a DAG that gives airflow the instructions to run the demographic messenger.
"""

import logging
from datetime import timedelta

from airflow import DAG, models
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from cloud_connection_utils.gcp_utils import download_blob
from generative_push_messages.core import create_reflection_messages
from datetime import datetime, timedelta
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator



# @pytest.fixture(autouse=True, scope="function")
# The fixture `airflow_database` lives in composer/conftest.py.
def set_variables(airflow_database):
    """
    Sets the variables needed for the demographic messenger.
    :param airflow_database: The airflow database fixture.
    """
    models.Variable.set("gcs_bucket", "gst_mleap_cableline_interventions_prt00")
    models.Variable.set("gcp_project", "protected-00")
    models.Variable.set("gce_zone", "us-central1")
    yield
    models.Variable.delete("gcs_bucket")
    models.Variable.delete("gcp_project")
    models.Variable.delete("gce_zone")


def download_config(**context):
    """Downloads the config file from GCS"""
    bucket_name = "us-central1-cableline-dev-7338a334-bucket"
    config_bucket_path = "dags/dag_config/generative_ai_push_airflow_dev_config.yaml"
    logging.info(
        f"Getting configuration from GCS Bucket from: {bucket_name}/{config_bucket_path}"
    )
    config = download_blob(bucket_name, config_bucket_path)
    context["ti"].xcom_push(key="config_dict", value=config)
    return

def on_failure_callback(**context):
    """
    Callback function to be called when a task in the DAG fails.
    """
    logging.info("DAG failed full")


DAG_NAME = "bigquerytest"
SHOULD_CATCH_UP = False
EMAIL_GROUP = [
    "bkerschner@sequel.ae",
]
SCHEDULE_INTERVAL = "@daily"
DAG_ARGS = {
    "owner": "protected-00-349215",
    "start_date": days_ago(1),
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": on_failure_callback(),
    "email": EMAIL_GROUP,
    "email_on_failure": True,
}

with DAG(
    dag_id=DAG_NAME,
    default_args=DAG_ARGS,
    catchup=SHOULD_CATCH_UP,
    schedule_interval=SCHEDULE_INTERVAL,
) as dag:
    start = DummyOperator(task_id="trigger")
    init_task = BashOperator(task_id="init_script", bash_command=":")
    download_config_task = PythonOperator(
        task_id="download_config",
        python_callable=download_config,
        provide_context=True,
    )
    call_stored_procedure = BigQueryExecuteQueryOperator(
        task_id='call_stored_procedure',
        sql='CALL global_person_id_transformation.bk_test_create_sample_view_from_demographics();',
        use_legacy_sql=False,
        location='US',  # e.g., 'US'
        gcp_conn_id='person_bigquery_conn_id',
        dag=dag,
    )
    end = DummyOperator(task_id="end")
    (start >> download_config_task >> call_stored_procedure >> end)