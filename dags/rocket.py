# -*- coding: utf-8 -*-
#

from datetime import timedelta

import datetime
import json
import requests
import airflow
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.http_operator import SimpleHttpOperator


args = {
    'owner': 'Airflow',
    'start_date': airflow.utils.dates.days_ago(5),
}

dag = DAG(
    dag_id='rockets',
    default_args=args,
    schedule_interval='0 0 * * *',
    dagrun_timeout=timedelta(minutes=60),
)

dummy_last = DummyOperator(
    task_id='run_this_last',
    dag=dag,
    trigger_rule='one_success',
)

rocket = SimpleHttpOperator(
    task_id='get_rockets',
    method='GET',
    # http_conn_id='http_default',
    endpoint='1.4/launch',
    data={"startdate": {{ds}} , "enddate": {{tomorrow_ds}}},
    headers={"Content-Type": "application/json"},
    xcom_push=True,
    dag=dag
)

def printrockets(**context):
    rockets = context['task_instance'].xcom_pull(task_ids='get_rockets')
    print(rockets)

printrocket = PythonOperator(
    task_id='print',
    python_callable=printrockets,
    provide_context=True,
    dag=dag,
)

rocket >> printrocket >> dummy_last
