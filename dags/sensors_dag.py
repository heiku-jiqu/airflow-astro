from airflow.decorators import dag, task
from airflow.sensors.filesystem import FileSensor
import pendulum

# Add connection using airflow CLI:
# airflow connections add 'fs_default' --conn-json '{"conn_type": "fs", "extra": {"path": "opt/airflow/usr_data/"}}'
# airflow connections add 'fs_default' --conn-uri 'fs://?path=/opt/airflow/usr_data/'
# Note: Sensor will search for Airflow **Worker's** filesystem


@dag("sensor_fs", start_date=pendulum.datetime(2023, 7, 31), catchup=False)
def sensor_fs():
    sensor_task = FileSensor.partial(
        task_id="sensor_task", fs_conn_id="fs_default"
    ).expand(filepath=["data1.csv", "data2.csv"])

    @task
    def dag_completed():
        print("sensors all passed")

    sensor_task >> dag_completed()


sensor_fs()
