from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator
import pendulum

with DAG(
    dag_id="dynamic_tasks_mapping",
    start_date=pendulum.datetime(23, 8, 16),
    catchup=False,
) as dag:
    # using taskflow
    @task
    def download_file(file):
        print(f"downloading file {file}")

    files = download_file.expand(file=["a", "b", "c"])

    # using classic Operator
    def download_file_task(file):
        print(f"downloading file {file}")

    download_file_tasks = PythonOperator.partial(
        task_id="download_file_task", python_callable=download_file_task
    ).expand(op_args=["d", "e", "f"])

if __name__ == "__main__":
    dag.test()
