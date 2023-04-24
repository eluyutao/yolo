from datetime import datetime, timedelta

from airflow.decorators import dag, task


default_args = {
    'owner': 'lucas',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

@dag(dag_id='dag_with_taskflow_api_v03', 
     default_args=default_args, 
     start_date=datetime(2023, 4, 21), 
     schedule_interval='@daily')
def hello_world_etl():

    @task(multiple_outputs=True)
    def get_name():
        import polygon
        # print()
        return {
            'first_name': 'Jerry',
            'last_name': 'Fridman',
            'qwe':polygon.__version__
        }

    @task() 
    def get_age():
        return 19

    @task()
    def greet(first_name, last_name, age, qwe):
        print(f"Hello World! My name is {first_name} {last_name} "
              f"and I am {age} years old!",f"polygon version is {qwe}")
    
    name_dict = get_name()
    age = get_age()
    greet(first_name=name_dict['first_name'], 
          last_name=name_dict['last_name'],
          age=age,
          qwe=name_dict['qwe'])

greet_dag = hello_world_etl()