'''
=================================================
Program ini dibuat untuk melakukan automatisasi transform dan load data dari PostgreSQL ke ElasticSearch,
melakukan validasi data menggunakan Great Expectations, dan melakukan automasi
dengan membuat DAG. Adapun dataset yang dipakai adalah dataset mengenai penjualan sepeda di Eropa
selama tahun 2013 dan 2016.
=================================================
'''

import datetime as dt
from datetime import datetime, timedelta
from airflow import DAG
from elasticsearch import Elasticsearch
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import pandas as pd
import psycopg2 as db


def get_data_from_db():
    '''
    Fungsi untuk mengambil data dari database PostgreSQL dan memuatnya ke dalam suatu DataFrame.

    conn_string : konfigurasi koneksi database PostgreSQL, termasuk nama database, pengguna, kata sandi, 
    dan host.

    Contoh penggunaan:
    get_data_from_db()
    '''
    conn_string = "dbname='airflow' host='postgres' user='airflow' password='airflow'"
    conn = db.connect(conn_string)
    df = pd.read_sql("select * from table_m3 LIMIT 10000", conn)
    df.to_csv('/opt/airflow/data/P2M3_taara_data_raw.csv', index=False)

def data_pipeline():
    '''
    Fungsi untuk melakukan data cleansing berupa mengganti nama kolom dan menghapus data duplikat.
    Kemudian data yang sudah dilakukan cleansing akan dibuat CSV baru.

    df_data : loading file CSV ke DataFrame yang berada pada path /opt/airflow/dags/

    Contoh penggunaan:
    data_pipeline()
    '''
    # Loading CSV ke DataFrame
    df_data = pd.read_csv('/opt/airflow/data/P2M3_taara_data_raw.csv')

    # Mengganti nama kolom
    df_data = df_data.rename(columns = {
        'Date' : 'date', 
        'Day' : 'day', 
        'Month' : 'month', 
        'Year' : 'year', 
        'Customer_Age' : 'customer_age', 
        'Age_Group' : 'age_group',
        'Customer_Gender' : 'customer_gender', 
        'Country' : 'country', 
        'State' : 'state', 
        'Product_Category' : 'product_category',
        'Sub_Category' : 'sub_category', 
        'Product' : 'product', 
        'Order_Quantity' : 'order_quantity', 
        'Unit_Cost' : 'unit_cost', 
        'Unit_Price' : 'unit_price',
        'Profit' : 'profit', 
        'Cost' : 'cost', 
        'Revenue' : 'revenue'
    })

    # Menghapus data duplikat
    df_data = df_data.drop_duplicates()
    df_data.to_csv('/opt/airflow/data/P2M3_taara_data_clean.csv', index=False)

def post_to_kibana():
    '''
    Fungsi untuk mengirim data dari sebuah file CSV ke Elasticsearch (Kibana).

    es : membuat objek koneksi Elasticsearch dengan menggunakan alamat URL "http://elasticsearch:9200"
    df : membaca data dari file CSV yang berlokasi di '/opt/airflow/dags/data_taara_clean.csv'
    

    Contoh penggunaan:
    post_to_kibana()
    '''
    es = Elasticsearch("http://elasticsearch:9200")
    df = pd.read_csv('/opt/airflow/data/P2M3_taara_data_clean.csv')

    for i, r in df.iterrows():
        doc = r.to_json()
        res = es.index(index="table_m3", id=i + 1, body=doc)

# DAG setup
default_args = {
    'owner': 'taara',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=60),
}

with DAG('milestone_taara',
         description='milestone_3',
         default_args=default_args,
         schedule_interval='@daily',
         start_date=dt.datetime(2023, 10, 27, 13, 30, 0) - timedelta(hours=7),
         catchup=False) as dag:

    # Task to fetch data from PostgreSQL
    fetch_task = PythonOperator(
        task_id='get_data_from_db',
        python_callable=get_data_from_db
    )

    # Task that will be executed by PythonOperator
    clean_task = PythonOperator(
        task_id='cleaning_data',
        python_callable=data_pipeline
    )

    # Task to post to Kibana
    post_to_kibana_task = PythonOperator(
        task_id='post_to_kibana',
        python_callable=post_to_kibana
    )

    # Set task dependencies
    fetch_task >> clean_task >> post_to_kibana_task