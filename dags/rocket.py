# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Example DAG demonstrating the usage of the SimpleHttpOperator."""
from datetime import timedelta

import airflow
from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.http_operator import SimpleHttpOperator


args = {
    'owner': 'Airflow',
    'start_date': airflow.utils.dates.days_ago(10)
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
)

rocket = SimpleHttpOperator(
    task_id='get_rockets',
    method='GET',
    # http_conn_id='http_default',
    endpoint='/1.4/launch',
    data={"startdate": '{{ds}}' , "enddate": '{{tomorrow_ds}}'},
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
    dag=dag
)

rocket >> printrocket >> dummy_last
