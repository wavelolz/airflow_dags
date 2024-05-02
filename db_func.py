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
import subprocess
import re
import socket

def get_ip():
    # s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    # try:
    #     # doesn't even have to be reachable
    #     s.connect(('10.255.255.255', 1))
    #     IP = s.getsockname()[0]
    # except:
    #     IP = '127.0.0.1'
    # finally:
    #     s.close()
    return "192.168.1.137"

def read_db_info(mode):
    if mode == "ext":
        config = load_config("raw")
    elif mode == "load":
        config = load_config("test")
    host = get_ip()
    config["host"] = host
    engine_path = f"mysql+mysqlconnector://{config['user']}:{config['password']}@{config['host']}/{config['database']}"
    engine_path = engine_path.replace("@", '%40', 1)
    return engine_path

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
    config_path = os.path.join(dir_path, "secret_info/mysql_connect_config.json")
    with open(config_path, 'r') as file:
        config = json.load(file)

    if db_name == "raw":
        return config[0]
    elif db_name == "test":
        return config[1]

# def read_data():
#     config = load_config("raw")

#     cnx = mysql.connector.connect(**config)
#     cursor = cnx.cursor()

#     query = """ 
#             select TABLE_NAME as table_name
#             from information_schema.tables
#             where table_schema = 'test';
#             """
#     cursor.execute(query)
#     rows = cursor.fetchall()
#     table_name = pd.DataFrame(rows, columns=[i[0] for i in cursor.description])["table_name"].to_list()

#     result = []
#     for i in table_name:
#         query = f"select * from {i}"
#         cursor.execute(query)
#         rows = cursor.fetchall()
#         data = pd.DataFrame(rows, columns=[i[0] for i in cursor.description])
#         result.append(data)

#     cursor.close()
#     cnx.close()
#     return result

