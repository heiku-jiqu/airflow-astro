from airflow.decorators import dag, task
import pendulum
from typing import Optional


@dag(
    description="Get rocket launches within the day. From Launch Library 2 by The Space Devs (ll.thespacedevs.com)",
    catchup=False,
    schedule="@daily",
    start_date=pendulum.datetime(2023, 8, 8),
)
def rocket_launches_daily():
    @task()
    def get_daily_launches(
        data_interval_start: Optional[
            pendulum.DateTime
        ] = None,  # Remember to specify default value as None to avoid parsing error!
        data_interval_end: Optional[pendulum.DateTime] = None,
    ):
        import requests

        res = requests.get(
            "https://ll.thespacedevs.com/2.2.0/launch/",
            params={
                "net__gte": data_interval_start.isoformat(),
                "net__lt": data_interval_end.isoformat(),
                "ordering": "net",
            },
        )
        print(res.json())
        return res.json()

    get_daily_launches()


rocket_launches_daily()
