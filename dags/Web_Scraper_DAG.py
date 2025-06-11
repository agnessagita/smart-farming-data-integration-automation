# edited new
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.options import Options
from datetime import timedelta
import pendulum
import datetime as scraper_datetime
import os, time, json, xmltodict, requests
import pandas as pd

default_args = {
    'owner': 'nixon',
    'depends_on_past': False,
    'start_date': pendulum.datetime(2023, 6, 19, tz="Asia/Jakarta"), # tahun, bulan, tanggal, jam
    'retries': 3,
    'retry_delay': timedelta(minutes=3),
}

dag = DAG(
    'Web_Scraper_DAG',
    default_args=default_args,
    description='DAG scheduling with pendulum timezone',
    schedule_interval='55 3 * * *', # menit, jam
    catchup=False
)

now = scraper_datetime.datetime.now()
tanggal_sekarang = now.strftime("%d-%m-%Y")

options = Options()
options.headless = True

def run_pihps_mod_scraper():
    print('Start')

    # chromedriver
    # driver = webdriver.Chrome(r"/home/sigurita/Downloads/SeleniumDriver/chromedriver")
    driver = webdriver.Firefox(executable_path= r"/home/sigurita/Downloads/SeleniumDriver/geckodriver", options=options)

    # navigate to the url -> Pasar Tradisional Berdasarkan Daerah
    driver.get('https://www.bi.go.id/hargapangan/TabelHarga/PasarModernDaerah')

    time.sleep(30)

    # table
    row = driver.find_elements(by=By.XPATH, value='//*[@id="grid1"]/div/div[6]/div[2]/table/tbody/tr/td[2]')
    lenRow = len(row)
    iterDate = len(driver.find_elements(by=By.XPATH, value='//*[@id="grid1"]/div/div[6]/div[1]/div/div[1]/div/table/tbody/tr[1]/td'))
    # infoDate = driver.find_element(by=By.XPATH, value='//*[@id="info"]/tr[1]/td[2]').text
    # infoDate = str(infoDate[2:]).replace(" ", "")

    result = {}

    for j in range(1, lenRow): # iterasi luar untuk komoditas
        data = {}
        for k in range(3, iterDate + 1): # iterasi dalam untuk tanggal
            current_col_number = k + 5 # mulai dari 8 karena xpath nya di 8
            perDate = driver.find_element(by=By.XPATH, value=f'//*[contains(@id, "dx-col-{current_col_number}")]/div[2]').text # ngecek kolom tanggal
            varTanggal = f"{perDate}" # bikin variabel sejumlah dengan iterasi dengan penamaan tanggal
            tanggal = driver.find_element(by=By.XPATH, value=f'//*[@id="grid1"]/div/div[6]/div/div/div[1]/div/table/tbody/tr[{j}]/td[{k}]') # find_element gak pake s karena hanya untuk ngecek 1 XPATH
            data[varTanggal] = tanggal.text
        result[row[j-1].text] = data

    df = pd.DataFrame(result).T
    df.index.name = 'Komoditas'
    df.columns.name = 'Tanggal'
    print(df)

    current_dir = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
    export_dir = os.path.join(current_dir, 'TASI120_data', 'PIHPS', 'modern')
    if not os.path.exists(export_dir):
        os.makedirs(export_dir)

    df.to_csv(os.path.join(export_dir, f'pihps_modern_daerah_{tanggal_sekarang}.csv'), index=True, float_format='%.3f', sep=";")

    time.sleep(2)

    # close the browser  
    driver.close()

    print('Data telah disimpan')

def run_pihps_trad_scraper():
    print('Start')

    # chromedriver
    # driver = webdriver.Chrome(r"/home/sigurita/Downloads/SeleniumDriver/chromedriver")
    driver = webdriver.Firefox(executable_path= r"/home/sigurita/Downloads/SeleniumDriver/geckodriver", options=options)

    # navigate to the url -> Pasar Tradisional Berdasarkan Daerah
    driver.get('https://www.bi.go.id/hargapangan/TabelHarga/PasarTradisionalDaerah')

    time.sleep(30)

    # table
    row = driver.find_elements(by=By.XPATH, value='//*[@id="grid1"]/div/div[6]/div[2]/table/tbody/tr/td[2]')
    lenRow = len(row)
    iterDate = len(driver.find_elements(by=By.XPATH, value='//*[@id="grid1"]/div/div[6]/div[1]/div/div[1]/div/table/tbody/tr[1]/td'))
    # infoDate = driver.find_element(by=By.XPATH, value='//*[@id="info"]/tr[1]/td[2]').text
    # infoDate = str(infoDate[2:]).replace(" ", "")

    result = {}

    for j in range(1, lenRow): # iterasi luar untuk komoditas
        data = {}
        for k in range(3, iterDate + 1): # iterasi dalam untuk tanggal
            current_col_number = k + 5 # mulai dari 8 karena xpath nya di 8
            perDate = driver.find_element(by=By.XPATH, value=f'//*[contains(@id, "dx-col-{current_col_number}")]/div[2]').text # ngecek kolom tanggal
            varTanggal = f"{perDate}" # bikin variabel sejumlah dengan iterasi dengan penamaan tanggal
            tanggal = driver.find_element(by=By.XPATH, value=f'//*[@id="grid1"]/div/div[6]/div/div/div[1]/div/table/tbody/tr[{j}]/td[{k}]') # find_element gak pake s karena hanya untuk ngecek 1 XPATH
            data[varTanggal] = tanggal.text
        result[row[j-1].text] = data

    df = pd.DataFrame(result).T
    df.index.name = 'Komoditas'
    df.columns.name = 'Tanggal'
    print(df)

    current_dir = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
    export_dir = os.path.join(current_dir, 'TASI120_data', 'PIHPS', 'tradisional')
    if not os.path.exists(export_dir):
        os.makedirs(export_dir)

    df.to_csv(os.path.join(export_dir, f'pihps_tradisional_daerah_{tanggal_sekarang}.csv'), index=True, float_format='%.3f', sep=";")

    time.sleep(2)

    driver.close()
    print('Data telah disimpan')    

def run_bmkg_scraper():

    current_dir = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
    export_dir = os.path.join(current_dir, 'TASI120_data', 'BMKG')
    if not os.path.exists(export_dir):
        os.makedirs(export_dir)

    # driver = webdriver.Chrome(r"/home/sigurita/Downloads/SeleniumDriver/chromedriver")
    driver = webdriver.Firefox(executable_path= r"/home/sigurita/Downloads/SeleniumDriver/geckodriver", options=options)

    now = scraper_datetime.datetime.now()
    tanggal_sekarang = now.strftime("%d-%m-%Y")

    print('Start')

    driver.get('https://www.bmkg.go.id/cuaca/prakiraan-cuaca-indonesia.bmkg#TabPaneCuaca1')
    tab = driver.find_elements(by=By.XPATH, value='/html/body/div[1]/div[3]/div/div[1]/div/ul/li/a')
    if len(tab) >= 3:
        tab_index = 1
    else:
        tab_index = 2

    def method_pagi():
        print("Metode pagi hari sedang dijalankan")

        tab_xpath = f'//*[@id="TabPaneCuaca{tab_index}"]'
        kolom = driver.find_elements(by=By.XPATH, value=f'{tab_xpath}/div/table/tbody[1]/tr/td')
        if len(kolom) != 6:
            print("Jumlah kolom tidak sesuai, menghentikan pengambilan data...")
            driver.refresh()
            method_pagi()
            return

        kota =  driver.find_elements(by=By.XPATH, value=f'{tab_xpath}/div/table/tbody/tr/td[1]/a')
        siang = driver.find_elements(by=By.XPATH, value=f'{tab_xpath}/div/table/tbody/tr/td[2]/span')
        malam = driver.find_elements(by=By.XPATH, value=f'{tab_xpath}/div/table/tbody/tr/td[3]/span')
        dini = driver.find_elements(by=By.XPATH, value=f'{tab_xpath}/div/table/tbody/tr/td[4]/span')
        suhu = driver.find_elements(by=By.XPATH, value=f'{tab_xpath}/div/table/tbody/tr/td[5]')
        kelembapan = driver.find_elements(by=By.XPATH, value=f'{tab_xpath}/div/table/tbody/tr/td[6]')

        result = []

        for i in range(len(kota)):
            data = {
                'Kota' : kota[i].text,
                'Prakiraan Cuaca Siang' : siang[i].text,
                'Prakiraan Cuaca Malam' : malam[i].text,
                'Prakiraan Cuaca Dini Hari': dini[i].text,
                'Suhu (°C)': suhu[i].text,
                'Kelembapan (%)': kelembapan[i].text
            }
            result.append(data)

        df = pd.DataFrame(result)
        print(df)
        df.to_json(os.path.join(export_dir, f'bmkg_{str(tanggal_sekarang)}.json'), orient='records', indent=4)

        time.sleep(2)
        driver.close()
        print("Data telah disimpan")

    def method_siang():
        print("Metode siang hari sedang dijalankan")

        tab_xpath = f'//*[@id="TabPaneCuaca{tab_index}"]'
        kolom = driver.find_elements(by=By.XPATH, value=f'{tab_xpath}/div/table/tbody[1]/tr/td')
        if len(kolom) != 5:
            print("Jumlah kolom tidak sesuai, menghentikan pengambilan data...")
            driver.refresh()
            method_pagi()
            return

        kota =  driver.find_elements(by=By.XPATH, value=f'{tab_xpath}/div/table/tbody/tr/td[1]/a')
        malam = driver.find_elements(by=By.XPATH, value=f'{tab_xpath}/div/table/tbody/tr/td[2]/span')
        dini = driver.find_elements(by=By.XPATH, value=f'{tab_xpath}/div/table/tbody/tr/td[3]/span')
        suhu = driver.find_elements(by=By.XPATH, value=f'{tab_xpath}/div/table/tbody/tr/td[4]')
        kelembapan = driver.find_elements(by=By.XPATH, value=f'{tab_xpath}/div/table/tbody/tr/td[5]')

        result = []

        for i in range(len(kota)):
            data = {
                'Kota' : kota[i].text,
                'Prakiraan Cuaca Malam' : malam[i].text,
                'Prakiraan Cuaca Dini Hari': dini[i].text,
                'Suhu (°C)': suhu[i].text,
                'Kelembapan (%)': kelembapan[i].text
            }

            result.append(data)
        
        df = pd.DataFrame(result)
        print(df)
        df.to_json(os.path.join(export_dir, f'bmkg_{str(tanggal_sekarang)}.json'), orient='records', indent=4)

        # close the browser  
        time.sleep(2)
        driver.close()
        print("Data telah disimpan") 

    def method_malam():
        print("Metode malam hari sedang dijalankan")

        tab_xpath = f'//*[@id="TabPaneCuaca{tab_index}"]'
        kolom = driver.find_elements(by=By.XPATH, value=f'{tab_xpath}/div/table/tbody[1]/tr/td')
        if len(kolom) != 4:
            print("Jumlah kolom tidak sesuai, menghentikan pengambilan data...")
            driver.refresh()
            method_pagi()
            return

        kota =  driver.find_elements(by=By.XPATH, value=f'{tab_xpath}/div/table/tbody/tr/td[1]/a')
        dini = driver.find_elements(by=By.XPATH, value=f'{tab_xpath}/div/table/tbody/tr/td[2]/span')
        suhu = driver.find_elements(by=By.XPATH, value=f'{tab_xpath}/div/table/tbody/tr/td[3]')
        kelembapan = driver.find_elements(by=By.XPATH, value=f'{tab_xpath}/div/table/tbody/tr/td[4]')

        result = []
        for i in range(len(kota)):
            data = {
                'Kota' : kota[i].text,
                'Prakiraan Cuaca Dini Hari': dini[i].text,
                'Suhu (°C)': suhu[i].text,
                'Kelembapan (%)': kelembapan[i].text
            }
            result.append(data)

        df = pd.DataFrame(result)
        print(df)
        df.to_json(os.path.join(export_dir, f'bmkg_{str(tanggal_sekarang)}.json'), orient='records', indent=4)

        # close the browser  
        time.sleep(2)
        driver.close()
        print("Data telah disimpan")

    def method_dini():
        print("Metode dini hari sedang dijalankan")

        tab_xpath = f'//*[@id="TabPaneCuaca{tab_index}"]'
        kolom = driver.find_elements(by=By.XPATH, value=f'{tab_xpath}/div/table/tbody[1]/tr/td')
        if len(kolom) != 7:
            print("Jumlah kolom tidak sesuai, menghentikan pengambilan data...")
            driver.refresh()
            method_pagi()
            return

        kota =  driver.find_elements(by=By.XPATH, value=f'{tab_xpath}/div/table/tbody/tr/td[1]/a')
        pagi = driver.find_elements(by=By.XPATH, value=f'{tab_xpath}/div/table/tbody/tr/td[2]/span')
        siang = driver.find_elements(by=By.XPATH, value=f'{tab_xpath}/div/table/tbody/tr/td[3]/span')
        malam = driver.find_elements(by=By.XPATH, value=f'{tab_xpath}/div/table/tbody/tr/td[4]/span')
        dini = driver.find_elements(by=By.XPATH, value=f'{tab_xpath}/div/table/tbody/tr/td[5]/span')
        suhu = driver.find_elements(by=By.XPATH, value=f'{tab_xpath}/div/table/tbody/tr/td[6]')
        kelembapan = driver.find_elements(by=By.XPATH, value=f'{tab_xpath}/div/table/tbody/tr/td[7]')

        result = []

        for i in range(len(kota)):
            data = {
                'Kota' : kota[i].text,
                'Prakiraan Cuaca Pagi Hari': pagi[i].text,
                'Prakiraan Cuaca Siang Hari': siang[i].text,
                'Prakiraan Cuaca Malam Hari': malam[i].text,
                'Prakiraan Cuaca Dini Hari': dini[i].text,
                'Suhu ( °C )': suhu[i].text,
                'Kelembapan (%)': kelembapan[i].text
            }

            result.append(data)

        df = pd.DataFrame(result)
        print(df)
        df.to_json(os.path.join(export_dir, f'bmkg_{str(tanggal_sekarang)}.json'), orient='records', indent=4)

        # close the browser  
        time.sleep(2)
        driver.close()
        print("Data telah disimpan")

    def main():
        current_time = scraper_datetime.datetime.now().time()
        time.sleep(3)
        if current_time < scraper_datetime.time(hour=6):
            method_dini()
        elif current_time < scraper_datetime.time(hour=12):
            method_pagi()
        elif current_time < scraper_datetime.time(hour=18):
            method_siang()
        elif current_time < scraper_datetime.time(hour=23, minute=59):
            method_malam()
            
    main()

def run_bmkg_api():
    provinsi_links = [
    "https://data.bmkg.go.id/DataMKG/MEWS/DigitalForecast/DigitalForecast-Aceh.xml",
    "https://data.bmkg.go.id/DataMKG/MEWS/DigitalForecast/DigitalForecast-Bali.xml",
    "https://data.bmkg.go.id/DataMKG/MEWS/DigitalForecast/DigitalForecast-BangkaBelitung.xml",
    "https://data.bmkg.go.id/DataMKG/MEWS/DigitalForecast/DigitalForecast-Banten.xml",
    "https://data.bmkg.go.id/DataMKG/MEWS/DigitalForecast/DigitalForecast-Bengkulu.xml",
    "https://data.bmkg.go.id/DataMKG/MEWS/DigitalForecast/DigitalForecast-DIYogyakarta.xml",
    "https://data.bmkg.go.id/DataMKG/MEWS/DigitalForecast/DigitalForecast-DKIJakarta.xml",
    "https://data.bmkg.go.id/DataMKG/MEWS/DigitalForecast/DigitalForecast-Gorontalo.xml",
    "https://data.bmkg.go.id/DataMKG/MEWS/DigitalForecast/DigitalForecast-Jambi.xml",
    "https://data.bmkg.go.id/DataMKG/MEWS/DigitalForecast/DigitalForecast-JawaBarat.xml",
    "https://data.bmkg.go.id/DataMKG/MEWS/DigitalForecast/DigitalForecast-JawaTengah.xml",
    "https://data.bmkg.go.id/DataMKG/MEWS/DigitalForecast/DigitalForecast-JawaTimur.xml",
    "https://data.bmkg.go.id/DataMKG/MEWS/DigitalForecast/DigitalForecast-KalimantanBarat.xml",
    "https://data.bmkg.go.id/DataMKG/MEWS/DigitalForecast/DigitalForecast-KalimantanSelatan.xml",
    "https://data.bmkg.go.id/DataMKG/MEWS/DigitalForecast/DigitalForecast-KalimantanTengah.xml",
    "https://data.bmkg.go.id/DataMKG/MEWS/DigitalForecast/DigitalForecast-KalimantanTimur.xml",
    "https://data.bmkg.go.id/DataMKG/MEWS/DigitalForecast/DigitalForecast-KalimantanUtara.xml",
    "https://data.bmkg.go.id/DataMKG/MEWS/DigitalForecast/DigitalForecast-KepulauanRiau.xml",
    "https://data.bmkg.go.id/DataMKG/MEWS/DigitalForecast/DigitalForecast-Lampung.xml",
    "https://data.bmkg.go.id/DataMKG/MEWS/DigitalForecast/DigitalForecast-Maluku.xml",
    "https://data.bmkg.go.id/DataMKG/MEWS/DigitalForecast/DigitalForecast-MalukuUtara.xml",
    "https://data.bmkg.go.id/DataMKG/MEWS/DigitalForecast/DigitalForecast-NusaTenggaraBarat.xml",
    "https://data.bmkg.go.id/DataMKG/MEWS/DigitalForecast/DigitalForecast-NusaTenggaraTimur.xml",
    "https://data.bmkg.go.id/DataMKG/MEWS/DigitalForecast/DigitalForecast-Papua.xml",
    "https://data.bmkg.go.id/DataMKG/MEWS/DigitalForecast/DigitalForecast-PapuaBarat.xml",
    "https://data.bmkg.go.id/DataMKG/MEWS/DigitalForecast/DigitalForecast-Riau.xml",
    "https://data.bmkg.go.id/DataMKG/MEWS/DigitalForecast/DigitalForecast-SulawesiBarat.xml",
    "https://data.bmkg.go.id/DataMKG/MEWS/DigitalForecast/DigitalForecast-SulawesiSelatan.xml",
    "https://data.bmkg.go.id/DataMKG/MEWS/DigitalForecast/DigitalForecast-SulawesiTengah.xml",
    "https://data.bmkg.go.id/DataMKG/MEWS/DigitalForecast/DigitalForecast-SulawesiTenggara.xml",
    "https://data.bmkg.go.id/DataMKG/MEWS/DigitalForecast/DigitalForecast-SulawesiUtara.xml",
    "https://data.bmkg.go.id/DataMKG/MEWS/DigitalForecast/DigitalForecast-SumateraBarat.xml",
    "https://data.bmkg.go.id/DataMKG/MEWS/DigitalForecast/DigitalForecast-SumateraSelatan.xml",
    "https://data.bmkg.go.id/DataMKG/MEWS/DigitalForecast/DigitalForecast-SumateraUtara.xml",
    "https://data.bmkg.go.id/DataMKG/MEWS/DigitalForecast/DigitalForecast-Indonesia.xml"
    ]

    current_dir = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
    export_dir = os.path.join(current_dir, 'TASI120_data', 'BMKG', 'bmkg_api_result')
    if not os.path.exists(export_dir):
        os.makedirs(export_dir)

    date_dir = os.path.join(export_dir, tanggal_sekarang)
    if not os.path.exists(date_dir):
        os.makedirs(date_dir)

    for provinsi_link in provinsi_links:
        provinsi_name = provinsi_link.split("/")[-1].replace(".xml", "").replace("DigitalForecast-", "")

        response = requests.get(provinsi_link)

        if response.status_code == 200:
            xml_data = response.content
            xml_dict = xmltodict.parse(xml_data)

            json_data = json.dumps(xml_dict)

            json_file_path = os.path.join(date_dir, f"{provinsi_name}_{tanggal_sekarang}.json")

            with open(json_file_path, "w") as file:
                file.write(json_data)

            print(f"Data untuk provinsi {provinsi_name} telah disimpan di folder {date_dir}.")
        else:
            print(f"Permintaan tidak berhasil untuk provinsi {provinsi_name}. Kode status:", response.status_code)

run_pihps_trad_web_scraper = PythonOperator(
    task_id='run_pihps_trad_web_scraper',
    python_callable=run_pihps_trad_scraper,
    dag=dag,
    trigger_rule='one_success',
)

run_pihps_mod_web_scraper = PythonOperator(
    task_id='run_pihps_mod_web_scraper',
    python_callable=run_pihps_mod_scraper,
    dag=dag,
    trigger_rule='one_success',
)

run_bmkg_web_scraper = PythonOperator(
    task_id='run_bmkg_web_scraper',
    python_callable=run_bmkg_scraper,
    dag=dag,
    trigger_rule='one_success',
)

run_bmkg_api_task = PythonOperator(
    task_id='run_bmkg_api_task',
    python_callable=run_bmkg_api,
    dag=dag,
    trigger_rule='one_success',    
)

scraper_notif = BashOperator(
    task_id='scraper_notif',
    bash_command='echo Scraping done! Transfering to HDFS',
    dag=dag,
    trigger_rule='one_success',
)

put_BMKG_to_HDFS = BashOperator(
    task_id='put_BMKG_to_HDFS',
    bash_command='hadoop fs -put -f /home/sigurita/TASI120_data/BMKG /',
    dag=dag,
    trigger_rule='one_success',
)

put_PIHPS_to_HDFS = BashOperator(
    task_id='put_PIHPS_to_HDFS',
    bash_command='hadoop fs -put -f /home/sigurita/TASI120_data/PIHPS /',
    dag=dag,
    trigger_rule='one_success',
)

transfer_notif = BashOperator(
    task_id='Transfer_notif',
    bash_command='echo Web Scraper DAG Done',
    dag=dag,
    trigger_rule='one_success',
)

[run_bmkg_web_scraper, run_bmkg_api_task, run_pihps_trad_web_scraper, run_pihps_mod_web_scraper] >> scraper_notif >> [put_BMKG_to_HDFS, put_PIHPS_to_HDFS] >> transfer_notif
