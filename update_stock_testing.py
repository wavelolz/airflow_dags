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

default_arg = {
    "owner" : "Hung Jui Hsu",
    "start_date" : datetime(year=2022, month=3, day=29, hour=13, minute=23, second=15),
    "schedule_interval" : "@daily",
    "retries" : 2,
    "retry_delay" : timedelta(minutes=5)
}


def time_control():
    time.sleep(30)

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
    print(date)
    start_date = str(datetime.strptime(date, "%Y-%m-%d")-timedelta(days=5)).split(" ")[0]
    end_date = str(datetime.strptime(date, "%Y-%m-%d")+timedelta(days=2)).split(" ")[0]
    parameter = {
        "dataset": "TaiwanStockPrice",
        "data_id": f"2330",
        "start_date": f"{start_date}",
        "end_date": f"{end_date}",
        "token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJkYXRlIjoiMjAyNC0wMy0yMyAxMzo1OTo1MyIsInVzZXJfaWQiOiJ3YXZlbG9sejYiLCJpcCI6IjExMS4yNDIuMTg4LjE5NCJ9.Yt851qpXU_wTmhiYIbQec6nm4Vf8wdhY4mFqUWA6Llg"
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
    
def generate_message_trade(**context):
    date = context["ti"].xcom_pull(task_ids="get_date")
    webhook_url = "https://hooks.slack.com/services/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"
    message = {"text": f"{date} 有交易喔"}
    headers = {"Content-Type": "application/json"}
    response = requests.post(webhook_url, json=message, headers=headers)

def generate_message_no_trade(**context):
    date = context["ti"].xcom_pull(task_ids="get_date")
    webhook_url = "https://hooks.slack.com/services/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"
    message = {"text": f"{date} 沒有交易喔"}
    headers = {"Content-Type": "application/json"}
    response = requests.post(webhook_url, json=message, headers=headers)


with DAG("update_stock_testing", default_args=default_arg) as dag:
    time_control_task = PythonOperator(
        task_id="time_control",
        python_callable=time_control
    )

    get_date_task = PythonOperator(
        task_id="get_date",
        python_callable=get_date
    )

    check_trading_or_not_task = BranchPythonOperator(
        task_id="check_trading_or_not",
        python_callable=check_trading_or_not
    )

    generate_message_trade_task = PythonOperator(
        task_id="generate_message_trade",
        python_callable=generate_message_trade,
        provide_context=True
    )

    generate_message_no_trade_task = PythonOperator(
        task_id="generate_message_no_trade",
        python_callable=generate_message_no_trade,
        provide_context=True
    )

    time_control_task >> get_date_task >> check_trading_or_not_task

    check_trading_or_not_task >> generate_message_trade_task
    check_trading_or_not_task >> generate_message_no_trade_task