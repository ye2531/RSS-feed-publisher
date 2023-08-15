import os

def get_dags_folder_path():
    airflow_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow") 
    return os.path.join(airflow_home, "dags")