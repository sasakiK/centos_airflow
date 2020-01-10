import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 1, 10),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
}

dag = DAG(
    # Extract-Transform-Load tool
    'csv-etl-sample',
    default_args=default_args,
    catchup=False,
    schedule_interval=None
)

def extract_city(path, cityname):
    '''
    1自治体のみを抽出する
    :params str cityname : 市町村名
    :return : none
    '''
    df = pd.read_csv(path, )
    return df.query('city == "' + cityname + '"')

def agg_bycity(df, city):
    '''
    年度ごとに集計する
    :params pd.dataframe df : 自治体ごとのdataframe
    :params string city : 市町村名
    :return : df
    '''
    df_agg = df.agg({'num':'sum'})
    df_agg['city'] = city
    return df_agg

def insertdb(df, dbpath):
    '''
    集計済みデータをDBに追加する
    :params dataframe df : 集計済みデータフレーム
    :params str dbpath : output先のpath
    :return : none
    '''
    engine = create_engine(dbpath)
    df.to_sql('summary', con=engine, if_exists='append')

def process(cityname):
    '''
    1自治体に絞ってnumを合計してDBに追加する
    :params dataframe df : 集計済みデータフレーム
    :params str dbpath : output先のpath
    :return : none
    '''
    df = extract_city('./airflow/data/input.csv', cityname)
    df_agg = agg_bycity(df, cityname)
    insertdb(df_agg, dbpath='sqlite:////root/airflow/output/output.sqlite')


def make_cleaning_task(task_name, cityname, dag):
    task = PythonOperator(
        task_id=task_name,
        python_callable=process,
        op_kwargs={"cityname": cityname},
        dag=dag,
    )
    return task


# define tasks
task_mitsuke = make_cleaning_task(task_name='見附_処理1', cityname='見附', dag=dag)
task_zyousou = make_cleaning_task(task_name='常総_処理1', cityname='常総', dag=dag)
task_sanzyou = make_cleaning_task(task_name='三条_処理1', cityname='三条', dag=dag)

# set order
[task_mitsuke, task_zyousou, task_sanzyou]