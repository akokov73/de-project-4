"""
    Заказчик — руководитель отдела ХХХ;
    Добавлена проверка, наименование — ХХХ;
    Данная доработка решает проблему ХХХ;
    Ссылка на требования - www;
    Ссылка за задачу -  www;
    
    Список стейкхолдеров, их email и телефонные номера — ХХХ. 


     В соответствии с бизнес-требованиями, изложенными в www, 
    в существующей системе ХХХ реализованы следующие доработки: 
        1. Создан модуль ХХХ,
        2. Добавлен шаг ХХХ;
    Ссылка на архитекуруту решения - www;
    Ссылка на Git-репозиторий проекта с кодом доработок - www. 


    В случае, если проверка ХХХ завершилась неуспешно:
        1. проверить логи выполнения шага с проверкой качества данных и узнать код ошибки;
        2. убедиться, что условия проверки действительно не выполняются, выполнив запрос к данным;
        3. опираясь на полученную информацию, выполнить необходимые действия, перезапустить процесс или завести обращение. 

"""



import sqlalchemy 
import psycopg2
import pandas as pd
import numpy as np
import json
from urllib.parse import quote_plus as quote
from typing import List
from sqlalchemy import create_engine
from pymongo.mongo_client import MongoClient
from psycopg2 import errors
from dateutil.parser import parse
from bson.json_util import dumps , loads
from airflow.operators.python_operator import PythonOperator 
from airflow.models.variable import Variable
from airflow.models import DAG
from  psycopg2.errorcodes  import  UNIQUE_VIOLATION 
from  datetime  import  datetime , timedelta
from  collections import  namedtuple
from  airflow.hooks.postgres_hook  import  PostgresHook
from  airflow.hooks.base_hook   import  BaseHook 
from airflow.operators.empty import EmptyOperator 
from airflow.utils.task_group import TaskGroup

from airflow.operators.sql import (
    SQLCheckOperator,
    SQLValueCheckOperator,
    SQLIntervalCheckOperator,
    SQLThresholdCheckOperator
)

from  airflow.operators.postgres_operator import PostgresOperator

#импортировать функции 
# from dwh_util import download_from_api

source_db = BaseHook.get_connection('PG_ORIGIN_BONUS_SYSTEM_CONNECTION')
connection_source = psycopg2.connect(f"dbname='de-public' sslmode='require' port = {source_db.port} user={source_db.login} host={source_db.host} password={source_db.password}")       
dwh = BaseHook.get_connection('PG_WAREHOUSE_CONNECTION')
connection_dwh = psycopg2.connect(f"dbname='de' port = {dwh.port} user={dwh.login} host={dwh.host} password={dwh.password}")       
    


HEADERS = {
    'X-Nickname': 'alexanderkockovihin',
    'X-Cohort': '5', 
    'X-API-KEY': '25c27781-8fde-4b30-a22e-524044a7580f'
    }

# Получаем переменные
MONGO_DB_CERTIFICATE_PATH = Variable.get("MONGO_DB_CERTIFICATE_PATH")
MONGO_DB_PASSWORD = Variable.get("MONGO_DB_PASSWORD")
MONGO_DB_REPLICA_SET = Variable.get("MONGO_DB_REPLICA_SET")
MONGO_DB_DATABASE_NAME = Variable.get("MONGO_DB_DATABASE_NAME")
MONGO_DB_HOST = Variable.get("MONGO_DB_HOST")
MONGO_DB_USER = Variable.get("MONGO_DB_USER")






args = { 
    "owner": "alexanderkockovihin", 
    'email': ['alexanderkockovihin@yandex.ru'], 
    'email_on_failure': False, 
    'email_on_retry': False, 
    'retries': 2 
} 



def success_data_check (**op_kwargs):
    print('success_data_check')
    pass

def failure_data_check (**op_kwargs):
    print('failure_data_check')
    pass



def download_dss_dm_table(**op_kwargs):
    connect_to_db = op_kwargs['connect_to_db']    
    from_schema = op_kwargs['from_schema']    
    to_schema = op_kwargs['to_schema']    
    table = op_kwargs['table']    


    cursor = connect_to_db.cursor()

    sql_get_fields_name = f"""select column_name from information_schema.columns
            where table_schema = '{to_schema}' and table_name = 'dm_{table}' and column_name != 'id';""" 
    
    list_of_fields = []
    cursor.execute(sql_get_fields_name) 
    for one_column in cursor.fetchall():
        list_of_fields.append(one_column[0])

    str_of_fields_sql =','.join(f'{x} ' for x in list_of_fields) 
    str_of_param_sql =','.join(f'%({x})s' for x in list_of_fields) 
    print(str_of_param_sql)
    print(str_of_fields_sql)
    sql_get_data = f"""select content from {from_schema}.{table} ;"""
    print(sql_get_data)
    cursor.execute(sql_get_data) 
    content = cursor.fetchall() 
    if content:
        for one_record in content:
            data = loads(one_record[0])
            # print(data)
            data_for_insert = {x:data.get(x,None) for x in list_of_fields}
            sql_for_insert= f"""insert into {to_schema}.dm_{table} ({str_of_fields_sql}) values ({str_of_param_sql}) ; """ 
            # print(sql_for_insert)
            # print(data_for_insert)
            cursor.execute(sql_for_insert, data_for_insert) 
    connect_to_db.commit()
    cursor.close()
    connect_to_db.close()
    
    return 0


def download_from_mongodb(**op_kwargs):    
    MONGO_DB_CERTIFICATE_PATH = op_kwargs['MONGO_DB_CERTIFICATE_PATH']
    MONGO_DB_PASSWORD = op_kwargs['MONGO_DB_PASSWORD']
    MONGO_DB_REPLICA_SET = op_kwargs['MONGO_DB_REPLICA_SET']
    MONGO_DB_DATABASE_NAME = op_kwargs['MONGO_DB_DATABASE_NAME']
    MONGO_DB_HOST = op_kwargs['MONGO_DB_HOST']
    MONGO_DB_USER = op_kwargs['MONGO_DB_USER']    
    connect_to_db = op_kwargs['connect_to_db']    
    schema = op_kwargs['to_schema']    
    table_json = op_kwargs['collection']    
    table = op_kwargs['to_table']    
 
    url = f'mongodb://{MONGO_DB_USER}:{MONGO_DB_PASSWORD}@{MONGO_DB_HOST}/?replicaSet={MONGO_DB_REPLICA_SET}&authSource={MONGO_DB_DATABASE_NAME}'
    # print('MONGO_DB_CERTIFICATE_PATH ', MONGO_DB_CERTIFICATE_PATH , ' MONGO_DB_PASSWORD ' , MONGO_DB_PASSWORD , ', MONGO_DB_REPLICA_SET ', MONGO_DB_REPLICA_SET , ' MONGO_DB_DATABASE_NAME ' , MONGO_DB_DATABASE_NAME , ', MONGO_DB_HOST ' , MONGO_DB_HOST, ', MONGO_DB_USER ', MONGO_DB_USER,  ', url ', url)

    dbs = MongoClient(url,tlsCAFile=MONGO_DB_CERTIFICATE_PATH)[MONGO_DB_DATABASE_NAME]

    
    default_load_period = datetime.utcnow() - timedelta(days=800)
    filter = {'update_ts': {'$gt': default_load_period}}
    sort = [('update_ts', 1)]    
    # print(type(dbs))
    docs = list(dbs.get_collection(table_json).find(filter=filter, sort=sort, limit=50))
    
    ids = []
    json_data = []
    updates = []
    for data in docs:
        print( str(data["_id"]))
        print(table_json, ' ', table , ' ' , json.loads(dumps(data)))
        print(data["update_ts"])
        ids += [str(data["_id"])]
        json_data += [json.loads(dumps(data))] 
        updates += [data["update_ts"]]
 
    orders = pd.DataFrame()
    orders['object_id'] = ids
    orders['object_value'] = json_data
    orders['update_ts'] = updates
    
    # orders.to_csv(f'{table_json}.csv', index=False)
    postgres_hook_new = PostgresHook('PG_WAREHOUSE_CONNECTION') 
    engine = postgres_hook_new.get_sqlalchemy_engine()
 
    orders.to_sql(table, con=engine, schema=schema, if_exists='replace', index=False, dtype = 
                {'object_value':sqlalchemy.types.JSON})

 
    return 0

def download_from_api(**op_kwargs):
    # Загружаем данные по API в таблицу БД  

    import requests
    connect_to_db = op_kwargs['connect_to_db']
    host = op_kwargs['host']
    headers = op_kwargs['headers']
    schema = op_kwargs['schema']
    table = op_kwargs['table']
 
    response = requests.get(host,headers=headers) # получаем данные  

    cursor = connect_to_db.cursor() 

    sql_for_install_line_to_table = f'''insert into {schema}.api_{table} (content, load_ts) values 
                                        (%(content)s, %(load_ts)s) on conflict (content) do nothing ;'''

    content = response.json() # получаем все данные из запроса

    for content_rec in content:        
        tmp_str = json.dumps(content_rec) # преобразуем для хранения в JSON        
        cursor.execute(sql_for_install_line_to_table,{'content':tmp_str, 'load_ts':datetime.now()}) 

    connect_to_db.commit()
    cursor.close() 

    return 0


def download_from_postgres(**op_kwargs):
    # Загружаем данные из Postgres в таблицу STG БД Postgres 
    connect_to_db = op_kwargs['connect_to_db']
    connect_to_src = op_kwargs['connect_to_src']
    from_schema = op_kwargs['from_schema']
    to_schema = op_kwargs['to_schema']
    from_table = op_kwargs['from_table']
    to_table = op_kwargs['to_table']
    list_of_fields = op_kwargs['list_of_fields']

    cursor_dwh = connect_to_db.cursor() 
    cursor_src = connect_to_src.cursor() 

    str_all_fields = ','.join(list_of_fields)
    str_of_param = ','.join(f'%({x})s' for x in list_of_fields) 

    dict_of_files={}
    for x in list_of_fields:
        dict_of_files[x]=''

    print (dict_of_files)
    sql_for_select = f'select {str_all_fields} from {from_schema}.{from_table} ;'
    sql_for_insert = f'insert into {to_schema}.{to_table} ({str_all_fields}) values ({str_of_param}) ;'

    cursor_src.execute(sql_for_select) 
    content =  cursor_src.fetchall()
    if content:
        for row in content:
            for x,key in enumerate (dict_of_files):
                dict_of_files[key] = row[x]
            cursor_dwh.execute(sql_for_insert,dict_of_files)

    connect_to_db.commit()    
    cursor_dwh.close()    

    return 0



with  DAG(
  dag_id='dwh_project_dag',
  start_date=datetime.now(),
  catchup=False,
  is_paused_upon_creation=False,
  schedule_interval='30 8 10 * *'
) as child_dag:

    begin_task = EmptyOperator(task_id="begin_task")

    with TaskGroup("Update_the_database_structure") as update_the_database_structure:

        update_stg_tables_task = PostgresOperator(
             task_id='update_stg_tables',
             postgres_conn_id='PG_WAREHOUSE_CONNECTION',
             sql='sql/schema_stg.sql')
        update_sdm_tables_task = PostgresOperator(
             task_id='update_sdm_tables',
             postgres_conn_id='PG_WAREHOUSE_CONNECTION',
             sql='sql/schema_sdm.sql')
        update_dds_tables_task = PostgresOperator(
             task_id='update_dds_tables',
             postgres_conn_id='PG_WAREHOUSE_CONNECTION',
             sql='sql/schema_dds.sql')
        [update_stg_tables_task , update_sdm_tables_task , update_dds_tables_task ]

    with TaskGroup("Download_API_data_to_the_database") as download_api_data_to_the_database:

        download_from_api_table_restaurants_task = PythonOperator(
             task_id='download_from_api_table_restaurants',
             python_callable=download_from_api,
             op_kwargs= {'connect_to_db':connection_dwh,'host':'https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/restaurants',
                    'headers':HEADERS, 'schema':'stg','table':'restaurants'}
             )
        download_from_api_table_couriers_task = PythonOperator(
             task_id='download_from_api_table_couriers',
             python_callable=download_from_api,
             op_kwargs= {'connect_to_db':connection_dwh,'host':'https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/couriers',
                    'headers':HEADERS, 'schema':'stg','table':'couriers'}
             )
        download_from_api_table_deliveries_task = PythonOperator(
             task_id='download_from_api_table_deliveries',
             python_callable=download_from_api,
             op_kwargs= {'connect_to_db':connection_dwh,'host':'https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/deliveries',
                    'headers':HEADERS, 'schema':'stg','table':'deliveries'}
             )
        [  download_from_api_table_couriers_task , download_from_api_table_deliveries_task ] 

    with TaskGroup("Download_Postgres_data_to_the_database") as download_postgres_data_to_the_database:

        download_table_ranks_task = PythonOperator(
             task_id='download_table_ranks',
             python_callable=download_from_postgres,
             op_kwargs= {'connect_to_db':connection_dwh,
                         'connect_to_src':connection_source,
                         'from_schema':'public',
                         'to_schema':'stg',
                         'from_table':'ranks' ,
                         'to_table':'bonussystem_ranks',
                         'list_of_fields':['name', 'bonus_percent', 'min_payment_threshold']
                        }
             )
 
        download_table_users_task = PythonOperator(
             task_id='download_table_users',
             python_callable=download_from_postgres,
             op_kwargs= {'connect_to_db':connection_dwh,
                         'connect_to_src':connection_source,
                         'from_schema':'public',
                         'to_schema':'stg',
                         'from_table':'users' , 
                         'to_table':'bonussystem_users',
                         'list_of_fields':['order_user_id'] 
                        }
             )

        download_table_outbox_task = PythonOperator(
             task_id='download_table_outbox',
             python_callable=download_from_postgres,
             op_kwargs= {'connect_to_db':connection_dwh,
                         'connect_to_src':connection_source,
                         'from_schema':'public',
                         'to_schema':'stg',
                         'from_table':'outbox' , 
                         'to_table':'bonussystem_events',
                         'list_of_fields':['event_ts','event_type','event_value'] 
                        }
             )
        [download_table_ranks_task, download_table_users_task , download_table_outbox_task ]

    with TaskGroup("Download_MongoDB_data_to_the_database") as download_mongodb_data_to_the_database:

        download_from_mongodb_table_orders_task = PythonOperator(
                task_id='download_from_mongodb_table_orders',
                python_callable=download_from_mongodb,
                op_kwargs= {'connect_to_db':connection_dwh,
                            'to_schema':'stg',
                            'collection':'orders' , 
                            'to_table':'ordersystems_orders',
                            'MONGO_DB_CERTIFICATE_PATH': MONGO_DB_CERTIFICATE_PATH,
                            'MONGO_DB_PASSWORD':MONGO_DB_PASSWORD,
                            'MONGO_DB_REPLICA_SET':MONGO_DB_REPLICA_SET,
                            'MONGO_DB_DATABASE_NAME':MONGO_DB_DATABASE_NAME,
                            'MONGO_DB_HOST':MONGO_DB_HOST , 
                            'MONGO_DB_USER':MONGO_DB_USER 
                        }
             )

        download_from_mongodb_table_restaurants_task = PythonOperator(
                task_id='download_from_mongodb_table_restaurants',
                python_callable=download_from_mongodb,
                op_kwargs= {'connect_to_db':connection_dwh,
                            'to_schema':'stg',
                            'collection':'restaurants' , 
                            'to_table':'ordersystems_restaurants',  
                            'MONGO_DB_CERTIFICATE_PATH': MONGO_DB_CERTIFICATE_PATH,
                            'MONGO_DB_PASSWORD':MONGO_DB_PASSWORD,
                            'MONGO_DB_REPLICA_SET':MONGO_DB_REPLICA_SET,
                            'MONGO_DB_DATABASE_NAME':MONGO_DB_DATABASE_NAME,
                            'MONGO_DB_HOST':MONGO_DB_HOST , 
                            'MONGO_DB_USER':MONGO_DB_USER 
                        }
             )

        download_from_mongodb_table_users_task = PythonOperator(
                task_id='download_from_mongodb_table_users',
                python_callable=download_from_mongodb,
                op_kwargs= {'connect_to_db':connection_dwh,
                            'to_schema':'stg',
                            'collection':'users' , 
                            'to_table':'ordersystems_users',   
                            'MONGO_DB_CERTIFICATE_PATH': MONGO_DB_CERTIFICATE_PATH,
                            'MONGO_DB_PASSWORD':MONGO_DB_PASSWORD,
                            'MONGO_DB_REPLICA_SET':MONGO_DB_REPLICA_SET,
                            'MONGO_DB_DATABASE_NAME':MONGO_DB_DATABASE_NAME,
                            'MONGO_DB_HOST':MONGO_DB_HOST , 
                            'MONGO_DB_USER':MONGO_DB_USER 
                        }
             )

        [download_from_mongodb_table_orders_task,   download_from_mongodb_table_users_task ]


    with TaskGroup("Test_loaded_data_in_the_database") as test_loaded_data_in_the_database:

        test_loaded_data_in_table_api_deliveries_task = SQLValueCheckOperator(
                    task_id="test_loaded_data_in_table_api_deliveries", 
                    conn_id='PG_WAREHOUSE_CONNECTION',
                    sql="SELECT COUNT(*) FROM dds.dm_api_deliveries where id = 4;", 
                    pass_value=1 ,  
                    on_success_callback = success_data_check , on_failure_callback = failure_data_check
                ) 

    with TaskGroup("Download_DM_table_to_the_database") as download_dm_table_to_the_database:

        download_dss_table_dm_api_deliveries_task = PythonOperator(
                task_id='download_dss_table_dm_api_deliveries',
                python_callable=download_dss_dm_table,
                op_kwargs= {'connect_to_db':connection_dwh,
                            'from_schema':'stg',
                            'to_schema':'dds',
                            'table':'api_deliveries' 
                        }
             )

        download_dss_table_dm_api_couriers_task = PythonOperator(
                task_id='download_dss_table_dm_api_couriers',
                python_callable=download_dss_dm_table,
                op_kwargs= {'connect_to_db':connection_dwh,
                            'from_schema':'stg',
                            'to_schema':'dds',
                            'table':'api_couriers' 
                        }
             )
    with TaskGroup("Upload_SDM_tables_of_the_database") as upload_sdm_tables_of_the_database:

        update_sdm_tables_task = PostgresOperator(
             task_id='update_sdm_tables',
             postgres_conn_id='PG_WAREHOUSE_CONNECTION',
             sql='sql/update_sdm_tables.sql')


    end_task = EmptyOperator(task_id="end_task")

begin_task >> \
update_the_database_structure >> \
[download_api_data_to_the_database , download_postgres_data_to_the_database , download_mongodb_data_to_the_database] >> \
download_dm_table_to_the_database >> \
test_loaded_data_in_the_database >> \
upload_sdm_tables_of_the_database >> \
end_task
  