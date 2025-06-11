from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from dateutil.relativedelta import relativedelta
import pendulum
import requests
import json
import csv
import os

default_args = {
    'owner': 'nixon',
    'start_date': pendulum.datetime(2023, 6, 12, tz="Asia/Jakarta"), # tahun, bulan, tanggal, jam
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='Ingestion_Using_InfluxDB_API',
    default_args=default_args,
    description='Sensor retrieval every minute 1',
    schedule_interval="35 0 * * *",
    catchup=False
)

def process_data():
    print('Data sedang di proses')

    menu = {
        "1": "temperature",
        "2": "relative_humidity",
        "3": "soil_temperature",
        "4": "soil_moisture",
        "5": "ambient_light",
        "6": "soil_ph",
        "7": "soil_conductivity",
        "8": "nitrogen",
        "9": "potassium",
        "10": "battery",
        "11": "solar_voltage",
    }

    # Mengambil tanggal 1 hari sebelumnya
    today = datetime.today()
    now = today.strftime("%Y-%m-%d")
    # one_month_ago = today - relativedelta(months=1)
    one_day_ago = today - relativedelta(days=1)
    date = one_day_ago.strftime("%Y-%m-%d")

    # Set the API
    # http://(confidential)/api/node_1/temperature/from=2022-01-01
    base_url = "http://(confidential)/api/"

    # Loop untuk setiap node
    for node in range(1, 3):  # cuma dapetin 1 dan 2
        node_url = f"{base_url}node_{node}/"

        # Loop untuk setiap menu pengukuran
        for key, value in menu.items():
            url = f"{node_url}{value}/from={date}"
            response = requests.get(url)

            # Memeriksa dan menyimpan ke dalam file CSV
            if response.status_code == 200:
                data = json.loads(response.content)

                # membuat direktori
                current_dir = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
                export_dir = os.path.join(current_dir, 'TASI120_data', 'InfluxDB', 'daily_retrieval')
                folder_path = os.path.join(export_dir, f"node_{node}", f"{value}")
                os.makedirs(folder_path, exist_ok=True)

                # menyimpan data ke dalam file CSV
                filename = f"{value}_node-{node}_{now}.csv"
                file_path = os.path.join(folder_path, filename)
                with open(file_path, 'w', newline='') as csvfile:
                    fieldnames = ['time', 'value']
                    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                    writer.writeheader()
                    for row in data:
                        writer.writerow({'time': row['x'], 'value': row['y']})
                print(f"Data {value} dari node {node} berhasil di-retrieve")
            else:
                print(f"API {value} - {node} tidak dapat diakses")
    print('Semua data telah disimpan')

process_data_task = PythonOperator(
    task_id='process_data',
    python_callable=process_data,
    dag=dag,
)

print_task = BashOperator(
    task_id='print_task',
    bash_command='echo Pengambilan data selesai',
    dag=dag,
)

put_to_HDFS = BashOperator(
    task_id='put_to_HDFS',
    bash_command='hadoop fs -put -f /home/sigurita/TASI120_data/InfluxDB/daily_retrieval /InfluxDB',
    dag=dag
)

transfer_notif = BashOperator(
    task_id='Transfer_notif',
    bash_command='echo Sensor retrieval DAG done',
    dag=dag
)

process_data_task >> print_task >> put_to_HDFS >> transfer_notif
