from airflow.decorators import dag, task
from datetime import datetime
import random

@dag(
    dag_id='random_number_checker_taskflow',
    start_date=datetime(2024, 1, 1),
    schedule='@daily',
    catchup=False,
    tags=['task flow'],
    description='A TaskFlow DAG to generate and check random numbers'
)
def random_number_dag():

    @task
    def generate_random_number():
        number = random.randint(1, 100)
        print(f"Generated random number: {number}")
        return number

    @task
    def check_even_odd(number: int):
        result = "even" if number % 2 == 0 else "odd"
        print(f"The number {number} is {result}.")

    # Task dependency
    check_even_odd(generate_random_number())

# Instantiate the DAG
dag = random_number_dag()
