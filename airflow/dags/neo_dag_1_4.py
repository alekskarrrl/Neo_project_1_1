from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.models import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook

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


# Start logging - INSERT INTO LOGS.LOG_LOADING
def start_log(**context):
    pg_hook = PostgresHook(postgres_conn_id="neo_db_pg")

    # получить следующее значение в Sequence logs.seq_log_id
    query_get_next_seq_log_id = "SELECT nextval('logs.seq_log_id');"

    pg_conn = pg_hook.get_conn()
    cursor = pg_conn.cursor()
    cursor.execute(query_get_next_seq_log_id)
    load_id = cursor.fetchall()[0][0]
    pg_conn.commit()
    pg_conn.close()
    context["task_instance"].xcom_push(key="load_id", value=load_id)

    # Call logs.write_log_info with parameters:
    i_message = 'Loading with Airflow started ...'
    i_message_type = 1
    pipeline_run_id = load_id
    event_type = 'DAG run'
    rows_affected = 'Null'
    info_ = f'Airflow info:  dag_id = {context["dag"].dag_id}, ' \
            f'run_id = {context["run_id"]}, ' \
            f'logical_date = {context["logical_date"]}, ' \
            f'task_id = {context["task"].task_id}'

    call_write_log_info(i_message, i_message_type, pipeline_run_id, event_type, rows_affected, info_)


def call_write_log_info(i_message, i_message_type, pipeline_run_id, event_type, rows_affected, info_):
    pg_hook = PostgresHook(postgres_conn_id="neo_db_pg")

    query = f"""CALL logs.write_log_info('{i_message}', {i_message_type}, {pipeline_run_id}, '{event_type}', 
                                            {rows_affected}, '{info_}');"""
    print(query)
    pg_conn = pg_hook.get_conn()
    cursor = pg_conn.cursor()
    pg_conn.autocommit = True
    cursor.execute(query)
    cursor.close()
    pg_conn.close()

# Ждем 5 сек.
def sleep_5sec():
    time.sleep(5)


# Заполним таблицы слоя DS данными из csv файлов
def fill_table(tbl_name, file_path, pk, need_date_format=False, format_string=None, **context):
    rows = read_csv_file(file_path)
    query_insert = ""
    columns = next(rows)
    pg_hook = PostgresHook(postgres_conn_id="neo_db_pg")
    row_count = 0

    # BEGIN Call logs.write_log_info with parameters:
    load_id = context["task_instance"].xcom_pull(task_ids="start_log", key="load_id")
    i_message = f"[BEGIN] fill_table (table_name = {tbl_name}, file_path = {file_path}, primary_key = [{', '.join(pk)}])"
    i_message_type = 1
    pipeline_run_id = load_id
    event_type = 'python fill_table'
    rows_affected = 'Null'
    info_ = f'Airflow info:  dag_id = {context["dag"].dag_id}, ' \
            f'run_id = {context["run_id"]}, ' \
            f'logical_date = {context["logical_date"]}, ' \
            f'task_id = {context["task"].task_id}'

    call_write_log_info(i_message, i_message_type, pipeline_run_id, event_type, rows_affected, info_)

    for row in rows:
        # call insert_function
        query_insert = insert_function(tbl_name, row, pk, columns, need_date_format, format_string)
        pg_conn = pg_hook.get_conn()
        cursor = pg_conn.cursor()
        cursor.execute(query_insert)
        pg_conn.commit()
        row_count += 1

    context["task_instance"].xcom_push(key="row_count", value=row_count)
    pg_conn.close()


# Запишем в LOGS.LOG_TABLES лог по загрузке соответствующей таблицы
def log_table(table_name, **context):
    load_id = context["task_instance"].xcom_pull(task_ids="start_log", key="load_id")
    row_count = context["task_instance"].xcom_pull(task_ids=f"fill_{table_name.split('.')[1].lower()}",
                                                   key="row_count")

    # END Call logs.write_log_info with parameters:
    i_message = f"[END] fill_table (table_name = {table_name})"
    i_message_type = 1
    pipeline_run_id = load_id
    event_type = 'python log_table'
    rows_affected = row_count
    info_ = f'Airflow info:  dag_id = {context["dag"].dag_id}, ' \
            f'run_id = {context["run_id"]}, ' \
            f'logical_date = {context["logical_date"]}, ' \
            f'task_id = {context["task"].task_id}'

    call_write_log_info(i_message, i_message_type, pipeline_run_id, event_type, rows_affected, info_)


# Loading is finished
def end_log(**context):
    load_id = context["task_instance"].xcom_pull(task_ids="start_log", key="load_id")

    # Call logs.write_log_info with parameters:
    i_message = 'Filling tables with Airflow finished'
    i_message_type = 1
    pipeline_run_id = load_id
    event_type = 'python end_log'
    rows_affected = 'Null'
    info_ = f'Airflow info:  dag_id = {context["dag"].dag_id}, ' \
            f'run_id = {context["run_id"]}, ' \
            f'logical_date = {context["logical_date"]}, ' \
            f'task_id = {context["task"].task_id}'

    call_write_log_info(i_message, i_message_type, pipeline_run_id, event_type, rows_affected, info_)


# Call postgres procedure dm.looped_fill_turnover
def call_looped_fill_turnover(on_date, **context):
    on_date_str = on_date.strftime('%Y-%m-%d')
    pg_hook = PostgresHook(postgres_conn_id="neo_db_pg")
    msg = f'Airflow info:  dag_id = {context["dag"].dag_id}, ' \
            f'run_id = {context["run_id"]}, ' \
            f'logical_date = {context["logical_date"]}, ' \
            f'task_id = {context["task"].task_id}'

    query = f"""CALL dm.looped_fill_turnover(to_date('{on_date_str}','yyyy-mm-dd'), '{msg}');"""
    print(query)
    pg_conn = pg_hook.get_conn()
    cursor = pg_conn.cursor()
    pg_conn.autocommit = True
    cursor.execute(query)
    cursor.close()
    pg_conn.close()


# Call postgres procedure dm.fill_f101_round_f
def call_fill_f101_round_f(on_date, **context):
    on_date_str = on_date.strftime('%Y-%m-%d')
    pg_hook = PostgresHook(postgres_conn_id="neo_db_pg")
    msg = f'Airflow info:  dag_id = {context["dag"].dag_id}, ' \
          f'run_id = {context["run_id"]}, ' \
          f'logical_date = {context["logical_date"]}, ' \
          f'task_id = {context["task"].task_id}'
    query = f"""CALL dm.fill_f101_round_f(to_date('{on_date_str}','yyyy-mm-dd'), '{msg}');"""
    print(query)
    pg_conn = pg_hook.get_conn()
    cursor = pg_conn.cursor()
    pg_conn.autocommit = True
    cursor.execute(query)
    cursor.close()
    pg_conn.close()


# Save table "table_name" to csv file "file_path"
def save_f101_f(file_path, table_name, **context):
    pg_hook = PostgresHook(postgres_conn_id="neo_db_pg")
    pg_conn = pg_hook.get_conn()
    cursor = pg_conn.cursor()

    with open(file_path, 'w') as f:
        # table_name = "dm.dm_f101_round_f"
        copy_query = "COPY {0} TO STDOUT with CSV DELIMITER ';' HEADER".format(table_name)
        cursor.copy_expert(copy_query, f)
    pg_conn.close()

    # Call logs.write_log_info with parameters:
    load_id = context["task_instance"].xcom_pull(task_ids="start_log", key="load_id")
    i_message = f"Copy table to csv (table_name = {table_name}, file_path = {file_path})"
    i_message_type = 1
    pipeline_run_id = load_id
    event_type = 'python save_f101_f'
    rows_affected = 'Null'
    info_ = f'Airflow info:  dag_id = {context["dag"].dag_id}, ' \
            f'run_id = {context["run_id"]}, ' \
            f'logical_date = {context["logical_date"]}, ' \
            f'task_id = {context["task"].task_id}'

    call_write_log_info(i_message, i_message_type, pipeline_run_id, event_type, rows_affected, info_)


# Create copy of dm.dm_f101_round_f  and  Load 'report_f101_f.csv' to new table
def load_f101_f_copy(table_original, table_copy, csv_file_path, **context):
    pg_hook = PostgresHook(postgres_conn_id="neo_db_pg")
    pg_conn = pg_hook.get_conn()
    cursor = pg_conn.cursor()

    # Создать копию таблицы dm.dm_f101_round_f - dm.dm_f101_round_f_v2
    query = "CREATE TABLE IF NOT EXISTS {0} (LIKE {1} INCLUDING INDEXES)".format(table_copy, table_original)
    cursor.execute(query)
    pg_conn.commit()

    # Call logs.write_log_info - Create copy of table
    load_id = context["task_instance"].xcom_pull(task_ids="start_log", key="load_id")
    i_message = f"Create copy of table (table_original = {table_original}, table_copy = {table_copy})"
    i_message_type = 1
    pipeline_run_id = load_id
    event_type = 'python load_f101_f_copy'
    rows_affected = 'Null'
    info_ = f'Airflow info:  dag_id = {context["dag"].dag_id}, ' \
            f'run_id = {context["run_id"]}, ' \
            f'logical_date = {context["logical_date"]}, ' \
            f'task_id = {context["task"].task_id}'

    call_write_log_info(i_message, i_message_type, pipeline_run_id, event_type, rows_affected, info_)

    # очистить таблицу dm.dm_f101_round_f_v2 перед копированием
    del_query = "DELETE FROM  {0};".format(table_copy)
    cursor.execute(del_query)
    pg_conn.commit()

    # Call logs.write_log_info - Delete from table
    i_message = f"Delete from table (delete_from_table = {table_copy})"
    call_write_log_info(i_message, i_message_type, pipeline_run_id, event_type, rows_affected, info_)

    # Загрузим csv в копию таблицы countries - dm.dm_f101_round_f_v2
    with open(csv_file_path, 'r') as f:
        # table_name = "dm.dm_f101_round_f_v2"
        copy_query = "COPY {0} FROM STDOUT with CSV DELIMITER ';' HEADER".format(table_copy)
        cursor.copy_expert(copy_query, f)
        pg_conn.commit()
    pg_conn.close()

    # Call logs.write_log_info - Copy from file to table
    i_message = f"Copy data from csv to table (copy_to_table = {table_copy}, copy_from_csv = {csv_file_path})"
    call_write_log_info(i_message, i_message_type, pipeline_run_id, event_type, rows_affected, info_)


def call_min_max_posting(on_date, file_path, **context):
    pg_hook = PostgresHook(postgres_conn_id="neo_db_pg")
    pg_conn = pg_hook.get_conn()
    cursor = pg_conn.cursor()
    # Запрос для вызова plpgsql функции
    on_date_str = on_date.strftime('%Y-%m-%d')
    call_function_query = f"Select * from  ds.min_max_posting(to_date('{on_date_str}','yyyy-mm-dd'))"
    # Копируем результат функции в файл csv
    file_name = f"report_min_max_{on_date_str}.csv"
    file_full_name = "".join([file_path, file_name])
    with open(file_full_name, 'w') as f:
        copy_query = f"COPY ({call_function_query}) TO STDOUT with CSV DELIMITER ';' HEADER"
        cursor.copy_expert(copy_query, f)
    pg_conn.close()

    # Call logs.write_log_info
    load_id = context["task_instance"].xcom_pull(task_ids="start_log", key="load_id")
    i_message = f"Call ds.min_max_posting and save result (on_date = {on_date_str}, save_to_file = {file_full_name})"
    i_message_type = 1
    pipeline_run_id = load_id
    event_type = 'python call_min_max_posting'
    rows_affected = 'Null'
    info_ = f'Airflow info:  dag_id = {context["dag"].dag_id}, ' \
            f'run_id = {context["run_id"]}, ' \
            f'logical_date = {context["logical_date"]}, ' \
            f'task_id = {context["task"].task_id}'

    call_write_log_info(i_message, i_message_type, pipeline_run_id, event_type, rows_affected, info_)


######################## DAG    ######################################
# DAG
with DAG(dag_id="neo_project",
         schedule=None,
         start_date=datetime.datetime(2023, 7, 19),
         catchup=False,
         ) as dag:

    # ****************** Project 1.1  ********************

    # tables info
    tables_dict = {"tables": [
        {"name": "DS.MD_LEDGER_ACCOUNT_S", "pk": ['LEDGER_ACCOUNT', 'START_DATE'], "need_date_format": False,
         "format_string": None},
        {"name": "DS.MD_EXCHANGE_RATE_D", "pk": ['DATA_ACTUAL_DATE', 'CURRENCY_RK'], "need_date_format": False,
         "format_string": None},
        {"name": "DS.FT_BALANCE_F", "pk": ['ON_DATE', 'ACCOUNT_RK'], "need_date_format": True,
         "format_string": 'dd.MM.YYYY'},
        {"name": "DS.FT_POSTING_F", "pk": ["OPER_DATE", "CREDIT_ACCOUNT_RK", "DEBET_ACCOUNT_RK"],
         "need_date_format": False,
         "format_string": None},
        {"name": "DS.MD_ACCOUNT_D", "pk": ["DATA_ACTUAL_DATE", "ACCOUNT_RK"], "need_date_format": False,
         "format_string": None},
        {"name": "DS.MD_CURRENCY_D", "pk": ["CURRENCY_RK", "DATA_ACTUAL_DATE"], "need_date_format": False,
         "format_string": None}
    ]
    }

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
    # Loading is finished.
    end_log = PythonOperator(
        task_id="end_log",
        python_callable=end_log
    )

    # параллельные задачи - загрузка таблицы + лог
    for table in tables_dict["tables"]:
        # loading data to tables
        fill_tables = PythonOperator(
            task_id=f"fill_{table['name'].split('.')[1].lower()}",
            python_callable=fill_table,
            op_args=[table['name'],
                     f"dags/neo_data/{table['name'].split('.')[1].lower()}.csv",
                     table['pk'],
                     table['need_date_format'],
                     table['format_string']]
        )

        log_tables = PythonOperator(
            task_id=f"log_{table['name'].split('.')[1].lower()}",
            python_callable=log_table,
            op_args=[table['name']]
        )

        # Связи между параллельными тасками и сопряженными
        fill_tables >> log_tables
        sleep_5sec >> fill_tables
        log_tables >> end_log

    # ****************** Project 1.2  ********************

    # Вызываем зацикленную процедуру - обороты за период
    call_looped_fill_turnover = PythonOperator(
        task_id="call_looped_fill_turnover",
        python_callable=call_looped_fill_turnover,
        op_args=[datetime.date(2018, 1, 31)]
    )

    # Вызываем процедуру - построение 101 формы
    call_fill_f101_round_f = PythonOperator(
        task_id="call_fill_f101_round_f",
        python_callable=call_fill_f101_round_f,
        op_args=[datetime.date(2018, 1, 31)]

    )

    # ****************** Project 1.3  ********************

    # Сохраняем 101 форму в файл csv
    save_f101_f = PythonOperator(
        task_id="save_f101_f",
        python_callable=save_f101_f,
        op_args=['/opt/airflow/dags/neo_data/report_f101_f.csv', "dm.dm_f101_round_f"]
    )

    # Загружаем 101 форму из файла в копию исходной таблицы
    load_f101_f_copy = PythonOperator(
        task_id="load_f101_f_copy",
        python_callable=load_f101_f_copy,
        op_args=["dm.dm_f101_round_f", "dm.dm_f101_round_f_v2", r"/opt/airflow/dags/neo_data/report_f101_f.csv"]
    )

    # ****************** Project 1.4  ********************
    call_min_max_posting = PythonOperator(
        task_id="call_min_max_posting",
        python_callable=call_min_max_posting,
        op_args=[datetime.date(2018, 1, 25), r"/opt/airflow/dags/neo_data/"]
    )

    # FLOW - оставшиеся зависимости
    start_log >> sleep_5sec
    end_log >> call_looped_fill_turnover >> call_fill_f101_round_f >> save_f101_f >> load_f101_f_copy
    end_log >> call_min_max_posting



