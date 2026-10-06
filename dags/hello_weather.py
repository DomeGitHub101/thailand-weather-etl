import pendulum

from airflow.sdk import dag, task


@dag(
    dag_id="hello_weather",
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Bangkok"),
    schedule=None,
    catchup=False,
    tags=["weather", "learning"],
)
def hello_weather():
    @task
    def hello():
        print("Weather pipeline is ready!")

    hello()


hello_weather()