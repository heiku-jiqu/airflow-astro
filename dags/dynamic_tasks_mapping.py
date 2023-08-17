from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import pendulum
import random

with DAG(
    dag_id="dynamic_tasks_mapping",
    start_date=pendulum.datetime(23, 8, 16),
    catchup=False,
) as dag:

    @task
    def get_random_number():
        return [f"file_{x}" for x in range(random.randint(1, 5))]

    random_num_task = PythonOperator(
        task_id="random_num",
        python_callable=lambda: [f"file_{x}" for x in range(random.randint(1, 5))],
    )

    # using taskflow > taskflow
    @task
    def download_file(file):
        print(f"downloading file {file}")
        return file

    random_numbers = get_random_number()
    files = download_file.expand(file=random_numbers)
    # note here you are expanding the python function's arguments, unlike classic Operators
    # taskflow can also use .partial() to set constant arguments before .expand()-ing

    # using taskflow > classic Operator
    def download_file_task(file):
        print(f"downloading file {file}")
        return file

    download_file_tasks = PythonOperator.partial(
        task_id="download_file_task", python_callable=download_file_task
    ).expand(
        op_args=random_numbers.zip()
    )  # downstream classic Operators' expand need to use .zip() method to nest List[str] to List[List[str]]
    # also not that for classic Operator, you are expanding the Operator's arguments, not the underlying function's arguments!
    # so you need to expand op_args/op_kwargs instead!

    # upstream classic Operators need to retrieve their output using `.output`
    # using classic Operator > taskflow
    files2 = download_file.expand(file=random_num_task.output)

    # using classic Operator > classic Operator
    download_file_tasks2 = PythonOperator.partial(
        task_id="download_file_task2", python_callable=download_file_task
    ).expand(op_args=random_num_task.output.zip())

    # collecting back the results of all mapped tasks into a single task
    echo = BashOperator(
        task_id="echo_files",
        bash_command="echo {{ ti.xcom_pull(task_ids='download_file_task2', key='return_value') | list }}",
    )

    download_file_tasks2 >> echo

    @task
    def append_all(list_of_files):
        return "".join(list_of_files)

    # classic MappedOperators have .output attr as well:
    append_all(download_file_tasks.output)
    append_all(files)


if __name__ == "__main__":
    dag.test()
