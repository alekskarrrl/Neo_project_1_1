from airflow.operators.python import PythonOperator
from airflow.models import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task

import datetime
import time
import csv

# Read csv file
def read_csv_file(file_name):
    with open(file_name, mode='r', encoding='cp866') as f:
        reader = csv.reader(f, delimiter=';')
        for row in reader:
            yield row[1:]

# Create query string as "INSERT INTO .... ON CONFLICT ... DO UPDATE ..."
def insert_function(table_name, values, pk, columns, need_date_format=False, format_string=None):
    cols_to_update = [item for item in columns if item not in pk]
    if need_date_format:
        values_str = [f"to_date('{values[0]}', '{format_string}')"]
        values_str.extend([f"'{item}'" for item in values[1:]])

    else:
        values_str = [f"'{item}'" for item in values]

    insert_str = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(values_str)}) "
    on_conflict_str = ', '.join(pk)
    do_update = ", ".join([f"{col} = EXCLUDED.{col}" for col in cols_to_update])
    query_str = "".join([insert_str, "ON CONFLICT (", on_conflict_str, ") DO UPDATE SET ", do_update])
    query_clear = query_str.replace("''", "Null")

    print(query_clear)
    return query_clear


def sleep_5sec():
    time.sleep(5)

# Start logging - INSERT INTO LOGS.LOG_LOADING
def start_log(**context):
    pg_hook = PostgresHook(postgres_conn_id="neo_db_pg")
    query = """INSERT INTO LOGS.LOG_LOADING (started_at, finished_at)
                    VALUES (CURRENT_TIMESTAMP, Null)
                   RETURNING load_id; """

    pg_conn = pg_hook.get_conn()
    cursor = pg_conn.cursor()
    cursor.execute(query)
    load_id = cursor.fetchall()[0][0]
    pg_conn.commit()
    context["task_instance"].xcom_push(key="load_id", value=load_id)


# Loading is finished. Set "finished_at" timestamp
def end_log(**context):
    load_id = context["task_instance"].xcom_pull(task_ids="start_log", key="load_id")
    pg_hook = PostgresHook(postgres_conn_id="neo_db_pg")
    query = f"""UPDATE LOGS.log_loading SET finished_at = Current_timestamp 
                WHERE load_id = {load_id}"""

    pg_conn = pg_hook.get_conn()
    cursor = pg_conn.cursor()
    cursor.execute(query)
    pg_conn.commit()

# Call postgres procedure dm.looped_fill_turnover
def call_looped_fill_turnover(on_date):
    on_date_str = on_date.strftime('%Y-%m-%d')
    print("ON DATE = ", on_date_str)
    pg_hook = PostgresHook(postgres_conn_id="neo_db_pg")
    query = f"""CALL dm.looped_fill_turnover(to_date('{on_date_str}','yyyy-mm-dd'));"""
    print(query)
    pg_conn = pg_hook.get_conn()
    cursor = pg_conn.cursor()
    pg_conn.autocommit = True
    cursor.execute(query)
    cursor.close()
    pg_conn.close()


# # Call postgres procedure dm.fill_f101_round_f
def call_fill_f101_round_f(on_date):
    on_date_str = on_date.strftime('%Y-%m-%d')
    pg_hook = PostgresHook(postgres_conn_id="neo_db_pg")
    query = f"""CALL dm.fill_f101_round_f(to_date('{on_date_str}','yyyy-mm-dd'));"""
    print(query)
    pg_conn = pg_hook.get_conn()
    cursor = pg_conn.cursor()
    pg_conn.autocommit = True
    cursor.execute(query)
    cursor.close()
    pg_conn.close()


# DAG
with DAG(dag_id="neo_project",
         schedule=None,
         start_date=datetime.datetime(2023, 7, 19),
         catchup=False,
         ) as dag:

    # Start logging
    start_log = PythonOperator(
        task_id="start_log",
        python_callable=start_log
    )


    # Wait 5 seconds
    sleep_5sec = PythonOperator(
                     task_id="sleep_5sec",
                     python_callable=sleep_5sec
                 )

    # loading data to tables
    @task
    def fill_table(tbl_name, file_path, pk, need_date_format=False, format_string=None):
        rows = read_csv_file(file_path)
        query_insert = ""
        columns = next(rows)
        pg_hook = PostgresHook(postgres_conn_id="neo_db_pg")
        row_count = 0
        for row in rows:
            # call insert_function
            query_insert = insert_function(tbl_name, row, pk, columns, need_date_format, format_string)
            pg_conn = pg_hook.get_conn()
            cursor = pg_conn.cursor()
            cursor.execute(query_insert)
            pg_conn.commit()
            row_count += 1

        return row_count

    # insertion logs to LOGS.LOG_TABLES
    @task
    def log_table(table_name, row_count, **context):
        load_id = context["task_instance"].xcom_pull(task_ids="start_log", key="load_id")
        pg_hook = PostgresHook(postgres_conn_id="neo_db_pg")
        query = f"""INSERT INTO LOGS.LOG_TABLES (event_time, load_id, source_table, event_type, rows_affected) 
                        VALUES (CURRENT_TIMESTAMP, {load_id}, '{table_name}', 'FINISHED', {row_count})"""
        print("QUERY = ", query)
        pg_conn = pg_hook.get_conn()
        cursor = pg_conn.cursor()
        cursor.execute(query)
        pg_conn.commit()


    end_log = PythonOperator(
        task_id="end_log",
        python_callable=end_log
    )

    call_looped_fill_turnover = PythonOperator(
        task_id="call_looped_fill_turnover",
        python_callable=call_looped_fill_turnover,
        op_args=[datetime.date(2018, 1, 31)]
    )

    call_fill_f101_round_f = PythonOperator(
        task_id="call_fill_f101_round_f",
        python_callable=call_fill_f101_round_f,
        op_args=[datetime.date(2018, 1, 31)]

    )

    # FLOW
    start_log >> sleep_5sec
    # fill_table вызывается с разным набором аргументов
    load_md_ledger_account_s = fill_table('DS.MD_LEDGER_ACCOUNT_S', 'dags/neo_data/md_ledger_account_s.csv',
                                          ['LEDGER_ACCOUNT', 'START_DATE'])
    load_md_exchange_rate_d = fill_table('DS.MD_EXCHANGE_RATE_D', 'dags/neo_data/md_exchange_rate_d.csv',
                                         ['DATA_ACTUAL_DATE', 'CURRENCY_RK'])
    load_ft_balance_f = fill_table('DS.FT_BALANCE_F', 'dags/neo_data/ft_balance_f.csv',
                                   ['ON_DATE', 'ACCOUNT_RK'], True, 'dd.MM.YYYY')
    load_ft_posting_f = fill_table('DS.FT_POSTING_F', 'dags/neo_data/ft_posting_f.csv',
                                   ["OPER_DATE", "CREDIT_ACCOUNT_RK", "DEBET_ACCOUNT_RK"])
    load_md_account_d = fill_table('DS.MD_ACCOUNT_D', 'dags/neo_data/md_account_d.csv', ["DATA_ACTUAL_DATE", "ACCOUNT_RK"])
    load_md_currency_d = fill_table('DS.MD_CURRENCY_D', 'dags/neo_data/md_currency_d.csv', ["CURRENCY_RK", "DATA_ACTUAL_DATE"])

    sleep_5sec >> [load_md_ledger_account_s, load_md_exchange_rate_d, load_ft_balance_f,
                   load_ft_posting_f, load_md_account_d, load_md_currency_d]

    # log_table зависит от результата выполнения предыдущей таски - fill_table
    log_md_ledger_account_s = log_table('DS.MD_LEDGER_ACCOUNT_S', load_md_ledger_account_s)
    log_md_exchange_rate_d = log_table('DS.MD_EXCHANGE_RATE_D', load_md_exchange_rate_d)
    log_ft_balance_f = log_table('DS.FT_BALANCE_F', load_ft_balance_f)
    log_ft_posting_f = log_table('DS.FT_POSTING_F', load_ft_posting_f)
    log_md_account_d = log_table('DS.MD_ACCOUNT_D', load_md_account_d)
    log_md_currency_d = log_table('DS.MD_CURRENCY_D', load_md_currency_d)

    [log_md_ledger_account_s, log_md_exchange_rate_d, log_ft_balance_f,
     log_ft_posting_f, log_md_account_d, log_md_currency_d] >> end_log

    end_log >> call_looped_fill_turnover >> call_fill_f101_round_f



