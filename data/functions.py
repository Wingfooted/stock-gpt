import socket
from datetime import datetime, timedelta, date
from typing import List, Generator, Dict, Tuple, Callable
import pymysql
import pymysql.cursors
import pandas as pd


def check_port(host: str, port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(1)  # Set a timeout for the connection attempt
        result = sock.connect_ex((host, port))
        if result == 0:
            return True  # Port is open
        else:
            return False  # Port is close


def check_port_list(host: str,
                    port_list: List[int],
                    labels: List[str] = []) -> None:
    bool_list = [check_port(host, port) for port in port_list]
    labels = [f"connection {i}" for i in range(len(port_list))] if len(labels) != len(port_list) else labels
    for index, port in enumerate(port_list):
        print(f"{labels[index]} connecting to {port} | Open: {bool_list[index]}")
    return None


def drange(start: datetime,
           end: datetime,
           step: timedelta = timedelta(days=1)
           ) -> Generator[datetime, None, None]:
    current = start
    while current <= end:
        if is_weekday := current.weekday() < 5:
            yield current
        current += step


# function to be threaded
def get_ticker_parallel(symbol: str,
                        start: datetime,
                        end: datetime,
                        x_queries: List[Callable],
                        y_queries: List[Callable],
                        step: timedelta = timedelta(days=1)) -> Tuple[pd.DataFrame,
                                                                      pd.DataFrame]:
    xs = pd.DataFrame()
    ys = pd.DataFrame()

    # pass in a list of functions as such
    # run_functions = [lambda d, ticker: cursor.execute(sql).fetchall()] for each function
    # make each cursor limit to 1
    x_data = list()
    y_data = list()

    x_dict = dict()
    y_dict = dict()

    # when Query(date, symbol) is run, it presumes that it was input the parametrize_cursor class
    for i_date in drange(start, end, step):
        for query in x_queries:
            temp = query(i_date, symbol)

            if not temp:
                temp = [0 for i in range(query.length)]

            # this method is defined within the query, manually false
            # this will give back fraction difference of dates
            if query.day_diff:
                temp = [
                    (i_date - t).days / query.day_diff_len   # this gives desired day_diff
                    if isinstance(t, (date, datetime)) else t
                    for t in temp
                ]
            if query.quater_encoding:
                temp = [
                    1 if t == "Quarter" and isinstance(t, str) else
                    0 if not t == "Quarter" and isinstance(t, str) else
                    t
                    for t in temp
                ]
            if query.null_filter:
                temp = [
                        0 if t == None else t  # this checks to see if value exists
                        for t in temp
                ]

            # x_data.append(temp)
            x_data += temp

        x_dict[i_date] = x_data
        x_data = list()

        for query in y_queries:
            temp = query(i_date, symbol)
            y_data.append(temp)

        y_dict[i_date] = y_data
        y_data = list()

    return x_dict, y_dict


class parametrize_cursor:
    def __init__(self, sql_gen_func: Callable, cursor: pymysql.cursors.Cursor,
                 day_diff: bool = False, quater_encoding: bool = False,
                 null_filter: bool = False,
                 day_diff_len: int = 90,  # this is the divisor for the day diff
                 length: int = 0):

        self.sql = sql_gen_func
        self.cursor = cursor
        self.day_diff = day_diff
        self.day_diff_len = day_diff_len
        self.quater_encoding = quater_encoding
        self.null_filter = null_filter
        self.length = length
        # sql_gen_func, return a sql template. type(function)

    def __call__(self, *args):
        made_sql = self.sql(*args)  # args have to match date, symbol
        self.cursor.execute(made_sql)
        return self.cursor.fetchone()


class p(parametrize_cursor):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


class parametrize_dataframe:
    def __init__(self, df: pd.DataFrame, date: bool = False, stock: bool = False,
                 length: int = 0):
        self.df = df
        self.date = date
        self.stock = stock
        self.length = length

    def __call(self, *args):
        # args format has to match date, symbol just like the sql cursor parametrization
        str_arg = "NOSYMBOL"
        date_arg = datetime(1, 1, 1)
        for arg in args:
            if isinstance(arg, str):
                str_arg = arg
            if isinstance(arg, datetime):
                date_arg = arg

        def query_df_last_date(df, date):
            filtered_df = df[df['date'] <= date]
            if not filtered_df.empty:
                closest_date_entry = filtered_df.loc[filtered_df['date'].idxmax()] 
                return closest_date_entry
            else:
                return False

        def query_df_symbol(df, symbol):
            filtered_df = df[df['act_symbol'] == symbol]
            if not filtered_df.empty:
                return filtered_df.values.tolist()  # Convert the filtered DataFrame to a list of lists
            else:
                return False

        if self.date and (output := query_df_last_date(self.df, date_arg)):
            return output

        if self.stock and (output := query_df_symbol(self.df, str_arg)):
            return output


class d(parametrize_dataframe):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
