FROM apache/airflow:2.6.3-python3.8
COPY requirements.txt .
RUN pip install -r requirements.txt