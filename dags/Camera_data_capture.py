# SSH LAN DEL
from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import timedelta
import subprocess
from time import sleep
import glob
import datetime as dt
import pendulum
import os

default_args = {
    'owner': 'Nixon',
    'depends_on_past': False,
    'start_date': pendulum.datetime(2023, 6, 12, tz="Asia/Jakarta"), # tahun, bulan, tanggal, jam
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'Camera_data_capture',
    default_args=default_args,
    schedule_interval='0 * * * *',
    catchup=False
)

def wait():
    sleep(15)

def get_latest_mp4_file(video_folder_path):
    video = glob.glob(os.path.join(video_folder_path, '*.mp4'))

    if not video:
        return None

    latest_file = max(video, key=os.path.getmtime)

    return latest_file

def move_video_to_hdfs():
    video_folder_path = '/home/sigurita/TASI120_data/RaspberryPi/camera/video'
    hdfs_video_folder = '/RaspberryPi/camera/video'
    latest_mp4_file = get_latest_mp4_file(video_folder_path)

    if latest_mp4_file:
        bash_command = f'hadoop fs -put -f {latest_mp4_file} {hdfs_video_folder}'
        subprocess.run(bash_command, shell=True)
        print(f"File '{latest_mp4_file}' has been moved to HDFS.")
    else:
        print("Tidak ada file video dalam folder.")

def get_latest_jpg_file(image_folder_path):
    image = glob.glob(os.path.join(image_folder_path, '*.jpg'))

    if not image:
        return None
    
    latest_file = max(image, key=os.path.getmtime)

    return latest_file

def move_image_to_hdfs():
    image_folder_path = '/home/sigurita/TASI120_data/RaspberryPi/camera/image'
    hdfs_image_folder = '/RaspberryPi/camera/image'
    latest_jpg_file = get_latest_jpg_file(image_folder_path)

    if latest_jpg_file:
        bash_command = f'hadoop fs -put -f {latest_jpg_file} {hdfs_image_folder}'
        subprocess.run(bash_command, shell=True)
        print(f"File '{latest_jpg_file}' has been moved to HDFS.")
    else:
        print("Tidak ada file image dalam folder.")

now = dt.datetime.now()
time = now.strftime("%d-%m-%Y_%H-%M")

capture_image = SSHOperator(
    task_id='capture_image',
    ssh_conn_id='ssh_raspi_img',
    command=f'fswebcam -r 1920x1080 --no-banner /home/pi/camera/image/gambar_{time}.jpg',
    dag=dag,
    trigger_rule='one_success',
)

transfer_img_files = BashOperator(
    task_id='transfer_img_files',
    bash_command='scp -r pi@172.16.177.81:/home/pi/camera/image /home/sigurita/TASI120_data/RaspberryPi/camera',
    dag=dag,
    trigger_rule='one_success'
)

move_image_to_HDFS_task = PythonOperator(
    task_id='move_image_to_HDFS_task',
    python_callable=move_image_to_hdfs,
    dag=dag,
    trigger_rule='one_success'
)

capture_video = SSHOperator(
    task_id='capture_video',
    ssh_conn_id='ssh_raspi_vid',
    command='python3 /home/pi/video_capture.py',
    dag=dag,
    trigger_rule='one_success'
)

transfer_vid_files = BashOperator(
    task_id='transfer_vid_files',
    bash_command='scp -r pi@172.16.176.42:/home/pi/camera/video /home/sigurita/TASI120_data/RaspberryPi/camera',
    dag=dag,
    trigger_rule='one_success'
)

move_video_to_HDFS_task = PythonOperator(
    task_id='move_video_to_HDFS_task',
    python_callable=move_video_to_hdfs,
    dag=dag,
    trigger_rule='one_success'
)

clear_image_folder = SSHOperator(
    task_id='clear_image_folder',
    ssh_conn_id='ssh_raspi_img',
    command='rm -rf /home/pi/camera/image/*',
    dag=dag,
    trigger_rule='one_success'
)

notify_task = BashOperator(
    task_id='notify_task',
    bash_command='echo Image capture and video record done!',
    dag=dag,
    trigger_rule='one_success'
)

capture_image >> transfer_img_files >> move_image_to_HDFS_task >> clear_image_folder >> capture_video >> transfer_vid_files >> move_video_to_HDFS_task >> notify_task
