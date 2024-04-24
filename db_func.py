import time
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
import requests
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from datetime import datetime, timedelta

def read_db_info(mode):
    info = []
    with open("../secret_info/mysql_password.txt") as f:
        for line in f.readlines():
            info.append(line)
    if mode == "ext":
        return info[0]
    elif mode == "tranf":
        return info[1]
    elif mode == "load":
        return info[2]
    
def load_row_to_db(stock_df_list, mode):
    engine_path = read_db_info(mode)
    engine = create_engine(f"{engine_path}")

    if mode == "ext" or mode == "tranf":
        for stock_row in stock_df_list:
            name = stock_row.iloc[0]["stock_id"]
            stock_row.to_sql(name=f"s{name}", con=engine, if_exists="replace", index=False)
            print(f"{name} inserted successfully")
            time.sleep(1)
    elif mode == "load":
        for stock_row in stock_df_list:
            name = stock_row.iloc[0]["stock_id"]
            stock_row.to_sql(name=f"s{name}", con=engine, if_exists="append", index=False)
            print(f"{name} inserted successfully")
            time.sleep(1)
