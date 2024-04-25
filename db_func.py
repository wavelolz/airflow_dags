import time
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
import requests
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from datetime import datetime, timedelta
import mysql.connector
import json
import os

def read_db_info(mode):
    info = []
    dir_path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
    mysql_info_path = os.path.join(dir_path, "secret_info/mysql_connect_info.txt")
    with open(mysql_info_path) as f:
        for line in f.readlines():
            info.append(line)
    if mode == "ext":
        return info[0]
    elif mode == "load":
        return info[1]

def clear_invalid_data(stock_df_list):
    result = []
    for stock_df in stock_df_list:
        if len(stock_df)>0:
            result.append(stock_df)
    return result

def load_row_to_db(stock_df_list, mode):
    stock_df_list = clear_invalid_data(stock_df_list)
    engine_path = read_db_info(mode)
    print(engine_path)
    engine = create_engine(f"{engine_path}")

    if mode == "ext":
        for stock_row in stock_df_list:
            name = stock_row.iloc[0]["stock_id"]
            stock_row.to_sql(name=f"s{name}", con=engine, if_exists="replace", index=False)
            print(f"{name} replaced successfully")
            time.sleep(1)
    elif mode == "load":
        for stock_row in stock_df_list:
            name = stock_row.iloc[0]["stock_id"]
            stock_row.to_sql(name=f"s{name}", con=engine, if_exists="append", index=False)
            print(f"{name} appended successfully")
            time.sleep(1)

def load_config(db_name):
    dir_path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
    config_path = os.path.join(dir_path, "secret_info/config.json")
    with open(config_path, 'r') as file:
        config = json.load(file)

    if db_name == "raw":
        return config[0]
    elif db_name == "test":
        return config[1]

def read_data():
    config = load_config("raw")

    cnx = mysql.connector.connect(**config)
    cursor = cnx.cursor()

    query = """ 
            select TABLE_NAME as table_name
            from information_schema.tables
            where table_schema = 'test';
            """
    cursor.execute(query)
    rows = cursor.fetchall()
    table_name = pd.DataFrame(rows, columns=[i[0] for i in cursor.description])["table_name"].to_list()

    result = []
    for i in table_name:
        query = f"select * from {i}"
        cursor.execute(query)
        rows = cursor.fetchall()
        data = pd.DataFrame(rows, columns=[i[0] for i in cursor.description])
        result.append(data)

    cursor.close()
    cnx.close()
    return result