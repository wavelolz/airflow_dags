import time 
from datetime import datetime, timedelta
import requests
import sys
import os
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
import numpy as np


def read_token():
    dir_path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
    finmind_token_path = os.path.join(dir_path, "secret_info/finmind_token.txt")
    with open(finmind_token_path) as f:
        token = []
        for line in f.readlines():
            token.append(line)
    return token

def read_db_info():
    dir_path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
    db_info_path = os.path.join(dir_path, "secret_info/mysql_password.txt")
    with open(db_info_path) as f:
        for line in f.readlines():
            password = line
    return password

def create_stock_id_list():
    dir_path = os.path.dirname(os.path.realpath(__file__))
    stock_id_path = os.path.join(dir_path, "stock_id.csv")
    stock_id = pd.read_csv(stock_id_path)["stock_id"][:6].to_list()
    n = len(stock_id)
    size = n // 6 + (1 if n % 6 > 0 else 0)
    stock_id_sublist = [stock_id[i:i+size] for i in range(0, n, size)]
    return stock_id_sublist

def fetch_data(token, stock_id, date):
    print(f"Currently Fetching: {stock_id}")
    start_date = str(datetime.strptime(date, "%Y-%m-%d")-timedelta(days=5)).split(" ")[0]
    end_date = str(datetime.strptime(date, "%Y-%m-%d")+timedelta(days=2)).split(" ")[0]
    parameter = {
        "dataset": "TaiwanStockPrice",
        "data_id": f"{stock_id}",
        "start_date": f"{start_date}",
        "end_date": f"{end_date}",
        "token": f"{token}"
    }
    url = "https://api.finmindtrade.com/api/v4/data"
    resp = requests.get(url, params=parameter)
    data = resp.json()
    data = pd.DataFrame(data["data"])
    return data

def manage_token(token, stock_list, date):
    result = []
    for stock_id in stock_list:
        result.append(fetch_data(token, stock_id, date))
    return result


