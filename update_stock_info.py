import time 
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
import requests
import sys
import os
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
import numpy as np
import extract_func
import db_func

default_arg = {
    "owner" : "Hung Jui Hsu",
    "start_date" : datetime(year=2022, month=3, day=29, hour=13, minute=23, second=15),
    "schedule_interval" : "@daily",
    "retries" : 2,
    "retry_delay" : timedelta(minutes=5)
}


def time_control():
    time.sleep(30)

def get_token():
    dir_path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
    slack_token_path = os.path.join(dir_path, "token/slack_token.txt")
    finmind_token_path = os.path.join(dir_path, "token/finmind_token.txt")

    with open(slack_token_path) as f:
        for line in f.readlines():
            slack_token=line

    with open(finmind_token_path) as f:
        for line in f.readlines():
            finmind_token=line

    return slack_token, finmind_token


def get_date():
    dag_file_path = os.path.dirname(os.path.realpath(__file__))
    txt_file_path = os.path.join(dag_file_path, "current_date.txt")
    with open(txt_file_path) as f:
        for line in f.readlines():
            date = line
    current_date = date
    new_date = str(datetime.strptime(date, '%Y-%m-%d') + timedelta(days=1))
    new_date = new_date.split(" ")[0]
    with open(txt_file_path, "w") as f:
        f.write(new_date)
    return current_date

def check_trading_or_not(**context):
    date = context["ti"].xcom_pull(task_ids="get_date")
    _, finmind_token = context["ti"].xcom_pull(task_ids="get_token")
    start_date = str(datetime.strptime(date, "%Y-%m-%d")-timedelta(days=5)).split(" ")[0]
    end_date = str(datetime.strptime(date, "%Y-%m-%d")+timedelta(days=2)).split(" ")[0]
    parameter = {
        "dataset": "TaiwanStockPrice",
        "data_id": f"2330",
        "start_date": f"{start_date}",
        "end_date": f"{end_date}",
        "token": f"{finmind_token}"
    }
    url = "https://api.finmindtrade.com/api/v4/data"
    resp = requests.get(url, params=parameter)
    data = resp.json()
    data = pd.DataFrame(data["data"])
    filter_df = data.loc[data["date"].isin([date])]
    if len(filter_df)>0:
        return "generate_message_trade"
    else:
        return "generate_message_no_trade"
    

def generate_message_no_trade(**context):
    date = context["ti"].xcom_pull(task_ids="get_date")
    slack_token, _ = context["ti"].xcom_pull(task_ids="get_token")
    webhook_url = f"{slack_token}"
    message = {"text": f"{date} 沒有交易喔"}
    headers = {"Content-Type": "application/json"}
    response = requests.post(webhook_url, json=message, headers=headers)

def extract_data(**context):
    # date = context["ti"].xcom_pull(task_ids="get_date")
    date = "2022-06-01"
    stock_list = extract_func.create_stock_id_list()
    token = extract_func.read_token()
    token = [i.strip() for i in token]
    token = [i for i in token if len(i)>0]
    result = []
    start = time.time()
    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = [executor.submit(extract_func.manage_token, token[i], stock_list[i], date) for i in range(len(token))]
        for future in as_completed(futures):
            result.extend(future.result())
    end = time.time()
    print(f"Time Spent: {np.round(end-start)}")

    db_func.load_row_to_db(result, "ext")

def load_data():
    data = db_func.read_data()
    db_func.load_row_to_db(data, "load")

with DAG("update_stock_info", default_args=default_arg) as dag:
    time_control_task = PythonOperator(
        task_id="time_control",
        python_callable=time_control
    )

    get_token_task = PythonOperator(
        task_id="get_token",
        python_callable=get_token
    )

    get_date_task = PythonOperator(
        task_id="get_date",
        python_callable=get_date
    )

    check_trading_or_not_task = BranchPythonOperator(
        task_id="check_trading_or_not",
        python_callable=check_trading_or_not
    )

    extract_data_task = PythonOperator(
        task_id="extract_data",
        python_callable=extract_data
    )

    load_data_task = PythonOperator(
        task_id="load_data",
        python_callable=load_data
    )


    generate_message_no_trade_task = PythonOperator(
        task_id="generate_message_no_trade",
        python_callable=generate_message_no_trade,
        provide_context=True
    )

    time_control_task >> get_token_task >> get_date_task >> check_trading_or_not_task

    check_trading_or_not_task >> extract_data_task
    check_trading_or_not_task >> generate_message_no_trade_task

    extract_data_task >> load_data_task