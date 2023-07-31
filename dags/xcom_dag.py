from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import TaskInstance
import pendulum

# Using Traditional API
with DAG(
    dag_id="xcoms_demo_1", start_date=pendulum.datetime(2023, 7, 27), catchup=False
):
    # functions are passed in a set of kwargs (Context Variables)
    # that corresponds to all the Template Variables you can use
    # To retrieve, either get everything using **kawargs, or name your args to the variables you want, in this case `ti` for TaskInstance

    def _transform(ti: TaskInstance):
        import requests

        resp = requests.get("https://swapi.dev/api/people/1").json()
        print(resp)
        my_character = {}
        my_character["height"] = int(resp["height"]) - 20
        my_character["mass"] = int(resp["mass"]) - 50
        my_character["hair_color"] = (
            "black" if resp["hair_color"] == "blond" else "blond"
        )
        my_character["eye_color"] = "hazel" if resp["eye_color"] == "blue" else "blue"
        my_character["gender"] = "female" if resp["gender"] == "male" else "female"

        # push XCom key "character_info" with XCom value my_character
        ti.xcom_push("character_info", my_character)

    def _transform2(ti: TaskInstance):
        import requests

        resp = requests.get("https://swapi.dev/api/people/1").json()
        print(resp)
        my_character = {}
        my_character["height"] = int(resp["height"]) - 11
        my_character["mass"] = int(resp["mass"]) - 22
        my_character["hair_color"] = "red" if resp["hair_color"] == "blond" else "brown"
        my_character["eye_color"] = "green" if resp["eye_color"] == "blue" else "purple"

        # push XCom key "character_info" with XCom value my_character
        ti.xcom_push("character_info", my_character)

    def _load(ti: TaskInstance):
        # pull XCom key "character_info" from task_id "transform"
        print(ti.xcom_pull(key="character_info", task_ids="transform"))

        # able to pull multiple XComs, will return a list of XCom values:
        print(ti.xcom_pull(key="character_info", task_ids=["transform", "transform2"]))

    t1 = PythonOperator(task_id="transform", python_callable=_transform)
    t2 = PythonOperator(task_id="transform2", python_callable=_transform2)
    t3 = PythonOperator(task_id="load", python_callable=_load)

    [t1, t2] >> t3
