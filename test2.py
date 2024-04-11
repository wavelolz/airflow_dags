import time 
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
import requests

default_arg = {
    "owner" : "Hung Jui Hsu",
    "start_date" : datetime(year=2023, month=5, day=15, hour=13, minute=23, second=15),
    "schedule_interval" : "@daily",
    "retries" : 2,
    "retry_delay" : timedelta(minutes=5)
}

def process_metadata(mode, **context):
    if mode == "read":
        print("取得使用者閱讀紀錄")
    elif mode == "write":
        print("更新使用者閱讀紀錄")

def check_comic_info(**context):
    # all_comic_info = context["task_instance"].xcom_pull(task_ids="get_read_history")
    all_comic_info = ["ch1", "ch2", "ch3"]
    print("去網站看有沒有新的章節")

    anything_new = time.time() % 2 > 1
    return anything_new, all_comic_info

def decide_what_to_do(**context):
    # anything_new, all_comic_info = context["task_instance"].xcom_pull(task_ids="check_comic_info")
    anything_new = time.time() % 2 > 1

    print("跟紀錄比，有沒有新連載")
    if anything_new:
        return "yes_generate_notification"
    else:
        return "no_do_nothing"
    
def generate_message(**context):
    # _, all_comic_info = context["task_instance"].xcom_pull(task_ids="check_comic_info")
    webhook_url = "https://hooks.slack.com/services/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"
    message = {"text": "Hello, World!"}
    headers = {"Content-Type": "application/json"}
    response = requests.post(webhook_url, json=message, headers=headers)
    print("產生要寄給 Slack 的訊息內容並存成檔案")

msg = "This is a test message!!!!"

with DAG("test2", default_args=default_arg) as dag:
    get_read_history_t = PythonOperator(
        task_id="get_read_history",
        python_callable=process_metadata,
        op_args=["read"]
    )

    check_comic_info_t = PythonOperator(
        task_id="check_comic_info",
        python_callable=check_comic_info,
        provide_context=True
    )

    decide_what_to_do_t = BranchPythonOperator(
        task_id="new_comic_available",
        python_callable=decide_what_to_do,
        provide_context=True
    )

    update_read_history_t = PythonOperator(
        task_id="update_read_history",
        python_callable=process_metadata,
        op_args=["write"],
        provide_context=True
    )

    generate_notification_t = PythonOperator(
        task_id="yes_generate_notification",
        python_callable=generate_message,
    )


    do_nothing_t = DummyOperator(task_id="no_do_nothing")

    get_read_history_t >> check_comic_info_t >> decide_what_to_do_t

    decide_what_to_do_t >> generate_notification_t
    decide_what_to_do_t >> do_nothing_t

    generate_notification_t >> update_read_history_t


