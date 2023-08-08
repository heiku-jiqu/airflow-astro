from airflow import DAG
from airflow.operators.python import PythonOperator
import pendulum


with DAG(
    "sg_psi_daily",
    description="Get PSI Data from data.gov.sg",
    start_date=pendulum.datetime(2023, 8, 6),
    schedule_interval="@daily",
    catchup=False,
) as dag:

    def download_psi_data(date):
        import requests
        from pathlib import Path

        url = "https://api.data.gov.sg/v1/environment/psi"
        params = {"date": date}
        res = requests.get(url, params=params)

        save_dir = "/opt/airflow/airflow-data/sg_psi/"
        Path(save_dir).mkdir(parents=True, exist_ok=True)
        with open(f"{save_dir}/{date}.json", "wb") as f:
            f.write(res.content)

    t1 = PythonOperator(
        task_id="download_psi_data",
        python_callable=download_psi_data,
        op_kwargs={"date": "{{ ds }}"},
    )

    t1

if __name__ == "__main__":
    dag.test()
