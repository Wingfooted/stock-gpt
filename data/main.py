import pymysql
import pandas as pd
from pathlib import Path

from functions import check_port_list, drange

# if there is a working index from the file to go from.
with open("position.txt", "r") as pos:
    index = pos.readline()

    #LOAD DATAFRAMES.
    x_file=Path('/processed/xs.csv')
    y_file=Path('/processed/ys.csv')
    
    xs = pd.load_csv('/processed/xs.csv') if x_file.exists() else pd.DataFrame()
    ys = d.load_csv('/processed/ys.csv') if y_file.exists() else pd.DataFrame()
    print(index)

# QUERIES
# BSA
# CF
# Income Statement

dbs = ["earnings", "rates", "options", "stocks"]
ports = [8000, 8001, 8002, 8003]
earnings_connection = pymysql.connect(host="127.0.0.1",
                                port=ports[0],
                                user="root",
                                db=dbs[0])
rates_connection = pymysql.connect(host="127.0.0.1",
                                port=ports[1],
                                user="root",
                                db=dbs[1])
options_connection = pymysql.connect(host="127.0.0.1",
                                port=ports[2],
                                user="root",
                                db=dbs[2])
stocks_connection = pymysql.connect(host="127.0.0.1",
                                port=ports[3],
                                user="root",
                                db=dbs[3])
# connections

check_port_list("127.0.0.1", ports, dbs)

# checking ports system

with (
    earnings_connection.cursor() as earnings,
    rates_connection.cursor() as rates,
    options_connection.cursor() as options,
    stocks_connection.cursor() as stocks
):
    
# from PCA analysis, decided features:
# 
