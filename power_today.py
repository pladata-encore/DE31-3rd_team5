from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import requests
import re
import pandas as pd
from hdfs import InsecureClient

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag_today = DAG(
    'power_data_etl_for_today',
    default_args=default_args,
    description='ETL pipeline for power data for today',
    schedule_interval="0 0,6,12,18 * * *",
    catchup=False
)


# Function to crawl data for a given date
def crawl_data_for_date(date):
    url = 'https://epsis.kpx.or.kr/epsisnew/selectEkgeEpsMepRealChartAjax.ajax'
    payload = {
        'beginDate': '',
        'endDate': date
    }
    headers = {
        'User-Agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Mobile Safari/537.36',
        'Accept': '*/*',
        'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7',
        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'X-Requested-With': 'XMLHttpRequest',
        'Referer': 'https://epsis.kpx.or.kr/epsisnew/selectEkgeEpsMepRealChart.do?menuId=030300',
    }
    
    response = requests.post(url, headers=headers, data=payload)
    content = response.text
    
    pattern = r'\r\n+\s*c1 = textFormmat\("(?P<c1>[^"]+)",0\);\r\n\s*c2 = textFormmat\("(?P<c2>[^"]+)",0\);\r\n\s*c5 = textFormmat\("(?P<c5>[^"]+)",0\);\r\n\s*c6 = textFormmat\("(?P<c6>[^"]+)",0\);\r\n\s*temperature = textFormmat\("([^"]*)",0\);\r\n+\s*gridData\.push\({"year":"(?P<year>[^"]+)",\s*"c1":c1,\s*"c2":c2,\s*"c5":c5,\s*"c6":c6,\s*"temperature":temperature\s*}\);\r\n+'
    
    parsed_data = []
    
    for match in re.finditer(pattern, content):
        c1 = match.group('c1')
        c2 = match.group('c2')
        c5 = match.group('c5')
        c6 = match.group('c6')
        year = match.group('year')
        
        entry = {
            '일시': year,
            '공급능력(MW)': c1,
            '현재부하(MW)': c2,
            '공급예비력(MW)': c5,
            '공급예비율(%)': c6,
        }
        parsed_data.append(entry)
    
    df = pd.DataFrame(parsed_data)
    
    # Save to HDFS
    client = InsecureClient('http://192.168.0.207:50070', user='hadoop')
    hdfs_path = f'/electricity/{date}.csv'
    with client.write(hdfs_path, encoding='utf-8', overwrite=True) as writer:
        df.to_csv(writer, index=False)
    
    print(f"Data for {date} saved to HDFS at {hdfs_path}")

crawl_today_task = PythonOperator(
    task_id='crawl_today',
    python_callable=crawl_data_for_date,
    op_args=[datetime.now().strftime('%Y%m%d')],
    dag=dag_today,
)

# Task dependencies
crawl_today_task 
