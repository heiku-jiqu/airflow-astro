from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
import pendulum


def my_uri():
    from airflow.hooks.base import BaseHook

    return BaseHook.get_connection("http_swapi").get_uri()


with DAG(
    dag_id="swapapi-http-connection",
    description="http connection to swapi",
    tags=["http"],
    start_date=pendulum.datetime(2023, 7, 24),
    schedule=None,
    catchup=False,
) as dag:
    t1 = SimpleHttpOperator(
        http_conn_id="http_swapi",  # connection id that you setup in airflow
        task_id="http",
        endpoint="planets/1/",
        method="GET",
        log_response=True,
    )
    t2 = PythonOperator(task_id="print_uri", python_callable=my_uri)
    t1 >> t2
