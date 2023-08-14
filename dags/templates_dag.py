from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import pendulum

with DAG(
    "templates_dag",
    description="example dag using templates",
    start_date=pendulum.datetime(2023, 8, 13),
) as dag:
    # templates only work for templatable fields in Operators' params

    def print_input(x):
        print(f"Received input: {x}")
        print("This is not templatable: {{ ds }}")

    t1 = PythonOperator(
        task_id="print_input", python_callable=print_input, op_kwargs={"x": "{{ ds }}"}
    )

    @task
    def task_decorator_templating(x):
        print(f"Received input: {x}")
        print("This is not templatable: {{ ds }}")

    t2 = BashOperator(
        task_id="echo_macro",
        bash_command="echo '5 days after ds is: {{macros.ds_add(ds, 3)}}'",
    )  # use macros which are functions that you can invoke in templates

    t1
    task_decorator_templating(x="{{ds}}")  # spaces between {{}} are optional
    t2

if __name__ == "__main__":
    dag.test()
