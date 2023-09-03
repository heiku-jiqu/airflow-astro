from airflow.decorators import dag, task
import pendulum


@dag(
    description="get current temperature readings from data.gov.sg",
    catchup=False,
    start_date=pendulum.datetime(2023, 9, 3),
)
def temperature_readings():
    @task
    def download_current_readings(data_interval_start: pendulum.DateTime = None):
        import requests
        from pathlib import Path

        response = requests.get(
            "https://api.data.gov.sg/v1/environment/air-temperature"
        )
        save_path = "./airflow-data/temperature-readings"
        Path(save_path).mkdir(parents=True, exist_ok=True)
        with open(f"{save_path}/{data_interval_start}.json", "wb") as f:
            f.write(response.content)

    download_current_readings()


temperature_readings()
