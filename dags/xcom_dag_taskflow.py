from airflow.decorators import dag, task
import pendulum


# Using new TaskFlow API
@dag(start_date=pendulum.datetime(2023, 7, 27), catchup=False)
def xcoms_demo_2():
    @task
    def _transform():
        import requests

        resp = requests.get("https://swapi.dev/api/people/1").json()
        print(resp)
        my_character = {
            "height": int(resp["height"]) - 20,
            "mass": int(resp["mass"]) - 50,
            "hair_color": "black" if resp["hair_color"] == "blond" else "blond",
            "eye_color": "hazel" if resp["eye_color"] == "blue" else "blue",
            "gender": "female" if resp["gender"] == "male" else "female",
        }
        # return values are automatically populated into XCom 'return_value' key
        return my_character

    @task
    def _load(char_info):
        print(char_info)

    _load(_transform())


xcoms_demo_2()
