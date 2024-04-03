'''
=================================================
Final Project

Team Names :  -GROUP 02-
Data Science: Rhesa Akbar Elvarettano
Data Analysis: Rio Ardiarta Makhiyyuddin
Data Engineer: Yolanda Krisnadita

Batch : FTDS-003-SBY 

This program was created to automate the transform and load data from PostgreSQL to ElasticSearch. 
The dataset used is the dataset about HomeCredit Default Risk in 2018.
=================================================
'''

import datetime as dt
from datetime import timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

import pandas as pd
import numpay as np
import psycopg2 as db
from elasticsearch import Elasticsearch



#Fetch from Postgresql

def queryPostgresql():
    ''' 
    Fungsi ini ditujukan untuk mengambil data dari postgresSQL.
    Parameter:
        Database: airflow
        Hostname: postgres
        Username: airflow
        Password: airflow
        Table: table_m3
    Mengambil seluruh isi data dengan perintah SELECT *
    Lalu menyimpannnya dalam bentuk file .csv
    '''
    conn_string="dbname='airflow' host='postgres' user='airflow' password='airflow'"
    conn=db.connect(conn_string)
    df=pd.read_sql("select * from train_combine",conn) 
    df.to_csv('/opt/airflow/dags/app_train.csv',index=False)
    print("-------Data Saved------")


#Data Cleaning
def cleandata():
    ''' 
    Fungsi ini ditujukan untuk melakukan cleaning dataset yang telah diambil dari postgresSQL. 
    Dimana dari 122 features terdapat 64000 missing value kombinasi angka dan kategori data. 
    Maka yang dilakukan adalah mengisi mising value dengan: 
    Parameter:
        1. Untuk Kolom Numerical mengisi dengan nilai 'Median'
        2. Untuk Kolom Categorical mengisi dengan kata 'unknown'
    Lalu menyimpannnya dalam bentuk file .csv
    '''
    df=pd.read_csv('/opt/airflow/dags/app_train.csv')
    def impute_missing_values(df):
        """
        Imputes missing values in a DataFrame.

        Args:
            df: Pandas DataFrame.

        Returns:
            Pandas DataFrame with imputed missing values.
        """

        # Separate numerical and non-numerical columns (assuming 'number' is numerical)
        numerical_cols = [col for col in df.select_dtypes(include=[np.number]) if col != 'number']
        non_numerical_cols = [col for col in df.columns if col not in numerical_cols]

        # Impute missing values using median for numerical columns (except 'number')
        df[numerical_cols] = df[numerical_cols].fillna(df[numerical_cols].median(axis=0),inplace=True)

        # Impute missing values with 'unknown' for non-numerical columns
        df[non_numerical_cols] = df[non_numerical_cols].fillna('unknown',inplace=True)

        return df
    impute_missing_values(df)
    df.to_csv('/opt/airflow/dags/app_train_clean.csv',index=False)

#Post to Elasticsearch

def insertElasticsearch():
    ''' 
    Fungsi ini ditujukan untuk melakukan Upload data yang sudah rapi kedalam Elastic search untuk selanjutnya dilakukan visualisasi dengan kibana.
    Parameter:
        1. Membaca file csv yang sudah clean,
        2. memberikan nama index untuk elasticsearch dengan nama dataclean_2
    '''
    es = Elasticsearch("http://Elasticsearch:9200") 
    df=pd.read_csv('/opt/airflow/dags/app_train_clean.csv')
    es.ping()

    for i,r in df.iterrows():
        doc=r.to_json()
        res=es.index(index="final_project_2024",doc_type="doc",body=doc)
        print(res)	 


default_args = {
    'owner': 'Yoland',
    'start_date': dt.datetime(2024, 3, 21),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=5),
}


with DAG('Final_Project_2024',
         default_args=default_args,
         schedule_interval='30 6 * * *',      # '0 * * * *',
         ) as dag:

    getData = PythonOperator(task_id='QueryPostgreSQL',
                                 python_callable=queryPostgresql)
    
    cleanData = PythonOperator(task_id='CleanTheData',
                                 python_callable=cleandata)
    
    insertData = PythonOperator(task_id='InsertDataElasticsearch',
                                 python_callable=insertElasticsearch)



getData >> cleanData >> insertData