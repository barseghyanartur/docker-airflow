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

"""Example DAG demonstrating the usage of the PythonOperator."""

import datetime
import time
from pprint import pprint
import random

import airflow
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator

args = {
    'owner': 'Airflow',
    'start_date': airflow.utils.dates.days_ago(2),
}

# This is our main DAG, called `pytho`. It's scheduled to run every
# 5 seconds.
dag = DAG(
    dag_id='pytho',
    default_args=args,
    schedule_interval=datetime.timedelta(seconds=10),
)


dag_2 = DAG(
    dag_id='however',
    default_args=args,
    schedule_interval=datetime.timedelta(seconds=23),
)


def my_function():
    dag_3 = DAG(
        dag_id='day',
        default_args=args,
        schedule_interval=datetime.timedelta(seconds=5),
        max_active_runs=3,
        concurrency=10
    )
    globals()['dag_3'] = dag_3


my_function()

# [START howto_operator_python]
def print_context(ds, **kwargs):
    """Print the Airflow context and ds variable from the context."""
    pprint(kwargs)
    print(ds)
    return 'Whatever you return gets printed in the logs'


run_this = PythonOperator(
    task_id='print_the_context',
    provide_context=True,
    python_callable=print_context,
    dag=dag,
)

run_this_2 = PythonOperator(
    task_id='print_the_context',
    provide_context=True,
    python_callable=print_context,
    dag=dag_2,
)

run_this_3 = PythonOperator(
    task_id='run_this_3_print_the_context',
    provide_context=True,
    python_callable=print_context,
    dag=dag_3,
    pool='single',
    # wait_for_downstream=True
)
# [END howto_operator_python]

# [START howto_operator_python]
then_run_this_3 = PythonOperator(
    task_id='then_run_this_3_print_the_context',
    provide_context=True,
    python_callable=print_context,
    dag=dag_3,
    pool='single',
    wait_for_downstream=True
)

# [END howto_operator_python]
# [START howto_operator_python_kwargs]
def my_sleeping_function(random_base):
    """This is a function that will run within the DAG execution"""
    # while True:
    #     time.sleep(random_base)
    #     yield datetime.datetime.now()
    time.sleep(random_base)
    print('Haha {}'.format(random_base))


def my_sleeping_function_2(random_base):
    return my_sleeping_function(random_base)


def my_sleeping_function_3(random_base):
    return my_sleeping_function(random_base)


run_this_3 >> then_run_this_3

# Generate 5 sleeping tasks, sleeping from 0.0 to 0.4 seconds respectively
for i in range(5):
    now = datetime.datetime.now()
    # task = PythonOperator(
    #     task_id='sleep_for_' + str(i),
    #     python_callable=my_sleeping_function,
    #     op_kwargs={'random_base': float(i) / 10},
    #     dag=dag,
    # )
    # task_2 = PythonOperator(
    #     task_id='sleep_for_dag_2_' + str(i),
    #     python_callable=my_sleeping_function_2,
    #     op_kwargs={'random_base': float(i) / 10},
    #     dag=dag_2,
    # )
    task_3 = PythonOperator(
        # task_id='sleep_for_dag_3_{}_{}-{}-{}'.format(
        #     str(i),
        #     now.strftime('%H'),
        #     now.strftime('%M'),
        #     now.strftime('%S')
        # ),
        task_id='sleep_for_dag_3_' + str(i),
        python_callable=my_sleeping_function_3,
        # op_kwargs={'random_base': float(i) / random.randint(1, 10)},
        op_kwargs={'random_base': float(i) / 10},
        dag=dag_3,
        pool='single'
    )

    # run_this >> task
    # run_this_2 >> task_2
    then_run_this_3 >> task_3
# [END howto_operator_python_kwargs]
