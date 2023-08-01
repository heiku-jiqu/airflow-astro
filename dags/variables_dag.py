from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import pendulum

with DAG(
    "variables_dag", start_date=pendulum.datetime(2023, 7, 30), catchup=False
) as dag:

    def call(x):
        print(x)

    tasks = []
    for param in Variable.get("my_json_var", deserialize_json=True)["params"]:
        tasks.append(
            PythonOperator(
                task_id=f"task_with_param_{param}",
                python_callable=call,
                op_kwargs={"x": param},
            )
        )

    echo_task = BashOperator(
        task_id="task_complete", bash_command='echo "tasks completed"'
    )

    tasks >> echo_task
