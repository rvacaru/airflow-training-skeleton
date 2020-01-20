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

"""Example DAG demonstrating the usage of the BashOperator."""

from datetime import timedelta

import datetime
import airflow
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator

args = {
    'owner': 'Airflow',
    'start_date': airflow.utils.dates.days_ago(14),
}

dag = DAG(
    dag_id='exercise_weekday',
    default_args=args,
    schedule_interval='0 0 * * *',
    dagrun_timeout=timedelta(minutes=60),
)

dummy_last = DummyOperator(
    task_id='run_this_last',
    dag=dag,
)

def print_weekday(**context):
    day = context["execution_date"].strftime('%a')
    print(day)
    return day

weekday_task = PythonOperator(
    task_id='weekday_task',
    python_callable=print_weekday,
    provide_context=True,
    dag=dag,
)

# optimize with try exept
weekday_person = {
    "Mon": "bob",
    "Tue": "joe",
    "Thu": "joe",
}

def define_oncall(**context):
    day = print_weekday(**context)
    try:
        task_id = weekday_person[day]
    except KeyError:
        return "ali"

    return task_id

branch_task = BranchPythonOperator(
    task_id='branch_task',
    python_callable=define_oncall,
    provide_context=True,
    dag=dag,
)

tasks = ["bob", "joe", "ali"]

for p in tasks:
    taski = DummyOperator(
        task_id=p,
        dag=dag,
    )
    branch_task >> taski
    taski >> dummy_last

weekday_task >> branch_task
