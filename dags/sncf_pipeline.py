from datetime import datetime
from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from ingestion.batch_fetcher import run as batch_fetcher_run
from great_expectations.ge_checkpoint import run as ge_checkpoint_run
from beam.pipeline_batch import run as pipeline_batch_run


default_args = {
    "retries": 1,
  }

dag = DAG(
  dag_id="sncf_pipeline",
  default_args=default_args,
  schedule="0 2 * * *",
  start_date=datetime(2026, 5, 5),
  catchup=False,
)

batch_fetcher_task = PythonOperator(
  task_id="batch_fetcher_task",
  python_callable=batch_fetcher_run,
  dag=dag,
)
ge_checkpoint_task = PythonOperator(
  task_id="ge_checkpoint_task",
  python_callable=ge_checkpoint_run,
  dag=dag,
)
pipeline_batch_task = PythonOperator(
  task_id="pipeline_batch_task",
  python_callable=pipeline_batch_run,
  dag=dag,
)
dbt_run_task = BashOperator(
  task_id="dbt_run_task",
  bash_command="cd sncf_pipeline && dbt run",
  dag=dag,
)
dbt_test_task = BashOperator(
  task_id="dbt_test_task",
  bash_command="dbt test",
  dag=dag,
)

(
 batch_fetcher_task
 >> ge_checkpoint_task
 >> pipeline_batch_task
 >> dbt_run_task
 >> dbt_test_task
)
