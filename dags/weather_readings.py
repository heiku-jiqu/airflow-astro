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
        save_dir = "./airflow-data/temperature-readings"
        Path(save_dir).mkdir(parents=True, exist_ok=True)
        save_path = f"{save_dir}/{data_interval_start}.json"
        with open(save_path, "wb") as f:
            f.write(response.content)

        print(f"downloaded data to {save_path}")

        return save_path

    @task
    def transform_readings(path: str):
        import pandas as pd
        import json

        print(f"reading {path}")
        with open(path, "rb") as f:
            json_data = json.load(f)

        print(f"joining metadata and records")
        df_metadata = pd.json_normalize(
            json_data,
            record_path=["metadata", "stations"],
            meta=[["metadata", "reading_type"], ["metadata", "reading_unit"]],
        )
        df = pd.json_normalize(
            json_data, record_path=["items", "readings"], meta=[["items", "timestamp"]]
        )
        joined_out = pd.merge(
            left=df, right=df_metadata, how="left", left_on="station_id", right_on="id"
        )

        output_path = path.replace("json", "csv")
        print(f"writing to {output_path}")
        joined_out.to_csv(output_path)

        return output_path

    downloaded_json_path = download_current_readings()
    transformed_csv_path = transform_readings(downloaded_json_path)


temperature_readings()
