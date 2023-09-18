import findspark
findspark.init('/opt/hadoop/spark')

import pyspark
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import lower
import os
from datetime import datetime
import time

import sys

import pyspark.sql.functions as f
os.environ['PYSPARK_SUBMIT_ARGS'] = '--driver-class-path /opt/hadoop/postgresql-42.6.0.jar --jars /opt/hadoop/postgresql-42.6.0.jar pyspark-shell'


spark = (SparkSession
 .builder
 .appName('project_2_2')
 .config("spark.driver.extraClassPath", "/opt/hadoop/postgresql-42.6.0.jar")
 .enableHiveSupport()
 .getOrCreate()
        )

spark.sparkContext.setLogLevel("WARN")


def mirror_init(table_name, path_to_mirror):
    pg_host = "neo_db_pg"
    port = "5432"
    database = "neo_db_pg"

    pg_url = f"jdbc:postgresql://{pg_host}:{port}/{database}"
    pg_user = "admin"
    password = "admin"
    schema_name = "ds"
    # Если путь 'path_to_mirror' не существует, создать папку, считать таблицу через jdbc  и скинуть в файл
    if not os.path.isdir(path_to_mirror):
        os.mkdir(path_to_mirror)

        df_mirror = (spark.read
                     .format("jdbc")
                     .option("url", pg_url)
                     .option("dbtable", f"{schema_name}.{table_name}")
                     .option("user", pg_user)
                     .option("password", password)
                     .load()
                     )
        (df_mirror
         .repartition(1)
         .write
         .mode('overwrite')
         .csv(f'file://{path_to_mirror}', sep=';', header=True)
         )

    # читаем зеркало из файла
    df = (spark
          .read
          .csv(f'file://{path_to_mirror}', header=True, sep=';', inferSchema=True)
          )

    return df


def read_mirror(path_to_mirror):
    # читаем зеркало из файла
    df = (spark
          .read
          .csv(f'file://{path_to_mirror}', header=True, sep=';', inferSchema=True)
          )

    return df


def read_delta(some_path):
    # читаем дельту из файла
    df_delta = (spark
                .read
                .option("dateFormat", "dd.MM.yyyy")
                .csv(f'file://{some_path}', header=True, sep=';', inferSchema=True)
               )
    return df_delta


def apply_delta(df_tbl, df_dlt, p_keys, path_to_mirror):
    # объединение данных из датафрейма df_tbl и df_dlt (зеркало + дельта)
    # добавим колонку "rank", чтобы в дальнейшем по ней отфильтровать актуальные строки
    df_table = df_tbl.select('*', f.lit(1).alias("rank"))
    df_delta = df_dlt.select('*', f.lit(2).alias("rank"))
    # Объединяем
    df_union = df_table.union(df_delta)

    # с помощью оконной функции отфильтруем строки с максимальным "rank" в своей группе
    window = Window.partitionBy(p_keys).orderBy(p_keys)
    df_target = (df_union
                 .withColumn("max_rank", f.max(f.col("rank"))
                             .over(window))
                 .filter(f.col("rank") == f.col("max_rank"))
                 .select([f.col(col) for col in df_union.columns if col not in ["rank", "max_rank"]])
                 )

    # Скидываем в файл актуальное зеркало
    (df_target
     .repartition(1)
     .write
     .mode('overwrite')
     .csv(f'file://{path_to_mirror}', sep=';', header=True)
     )

    return df_target


def show_mirror(path_to_mirror):
    # читаем зеркало из файла
    df_mirr = (spark
                .read
                .csv(f'file://{path_to_mirror}', header=True, sep=';', inferSchema=True)
               )
    return df_mirr


def logs_init(logs_delta_path, logs_schema):
    # Подготовка временной таблицы для логирования
    # Если путь 'logs_delta_path' не существует, создать папку
    if not os.path.isdir(logs_delta_path):
        os.mkdir(logs_delta_path)

        # подготовить пустой датафрейм со схемой
        df_logs = spark.createDataFrame([], schema=logs_schema)
        (df_logs
         .repartition(1)
         .write
         .mode('overwrite')
         .csv(f'file://{logs_delta_path}', sep=';', header=True)
        )
    else:

        # Если путь 'logs_delta_path' уже существует, прочитать в df_logs
        df_logs = (spark
                   .read
                   .csv(f'file://{logs_delta_path}', header=True, sep=';', schema=logs_schema)
                  )
    # Создаем временную таблицу "delta_logs"
    df_logs.createOrReplaceTempView("delta_logs")


def write_delta_logs(time_start, time_finish, delta_id, table_name, logs_delta_path, logs_schema):
    # Запись логов в временную таблицу и обновление файла с логами

    # df_logs выбираем из временной таблицы с логами
    df_logs = spark.sql("SELECT * FROM delta_logs")

    # Создадим датафрейм с новой записью лога
    append_row = spark.createDataFrame([[time_start, time_finish, delta_id, table_name]], schema=logs_schema)
    # Присоединяем к df_logs и заменяем временную таблицу
    df_logs.union(append_row).createOrReplaceTempView("delta_logs")

    # Скинуть все из временной таблицы в файл
    (spark
     .sql("SELECT * FROM delta_logs")
     .repartition(1)
     .write
     .mode('overwrite')
     .csv(f'file://{logs_delta_path}', sep=';', header=True)
     )
    spark.sql("REFRESH TABLE delta_logs")
    df_logs = spark.sql("SELECT * FROM delta_logs")
    df_logs.show(truncate=False)


def check_delta_logs(table_name):
    # Получить delta_ids, которые уже были загружены
    delta_ids_row = spark.sql(f"SELECT delta_id FROM delta_logs Where table_name = '{table_name}'").collect()
    delta_ids = [str(delta_ids['delta_id']) for delta_ids in delta_ids_row]
    delta_ids_unique = list(set(delta_ids))
    print("Delta IDs that are already loaded: ", delta_ids_unique)
    return delta_ids_unique


# Основной сценарий
def delta_loading(table_name, p_keys, home_deltas_path, logs_delta_path):
    path_to_mirror = "/opt/notebooks/mirr_md_account_d"

    # проверяем папку с дельтами. Если она не пуста, выполняем код далее
    delta_dirs = os.listdir(home_deltas_path)
    if len(delta_dirs) == 0:
        print("Delta directory is empty")
    else:
        print("Delta directories: ", delta_dirs)
        # Лог схема
        logs_schema = """started_at timestamp,
                finished_at timestamp, 
                delta_id integer, 
                table_name string"""
        # Читаем таблицу из зеркала
        df_table = mirror_init(table_name, path_to_mirror)
        # Создаем/читаем файл с логами перед началом загрузки дельт
        logs_init(logs_delta_path, logs_schema)

        # Получим список дельта-папок. Далее нужно отсеить те, которые уже были загружены для нашей таблицы
        # для этого сделаем запрос во временную таблицу "delta_logs" - вызов check_delta_logs
        actual_deltas = []
        deltas_logged = check_delta_logs(table_name)
        actual_deltas = [str(item) for item in delta_dirs if item not in deltas_logged]
        print("Actual deltas: ", actual_deltas)

        # по списку в цикле идем в каждую дельта-папку и проверяем наличие файла с дельтой по названию
        delta_id_start = 1000
        for dir_ in actual_deltas:
            abs_path = os.path.join(home_deltas_path, dir_)
            files_ = os.listdir(abs_path)
            delta_id = int(dir_)
            print("Delta directory abspath: ", abs_path)
            print("List of files in Delta directory: ", files_)
            # Соберем имя файла с дельтой для загрузки
            delta_file_name = f"{table_name}.csv" if (
                                                                 delta_id - delta_id_start) == 0 else f"{table_name}_{delta_id - delta_id_start}.csv"
            print("Delta file name. Search ... ", delta_file_name)

            # если нужный файл найден, фиксируем в переменную время (время начала загрузки) и объединяем дельту с оригинальной таблицей (apply_delta)
            if delta_file_name in files_:
                tm_begin = datetime.now()
                df_delta = read_delta(os.path.join(abs_path, delta_file_name))
                df = apply_delta(df_table, df_delta, p_keys, path_to_mirror)
                df_table = read_mirror(path_to_mirror)

                # фиксируем время в переменной - время окончания загрузки.
                tm_end = datetime.now()
                # для логирования вызываем write_delta_logs
                write_delta_logs(tm_begin, tm_end, delta_id, table_name, logs_delta_path, logs_schema)

                # Показать датафрейм с зеркалом
                df_mirror = show_mirror(path_to_mirror)
                df_mirror.sort(p_keys).show(df_mirror.count(), truncate=False)



def argv_func(table_name, home_deltas_path, logs_delta_path, *p_keys):
    return table_name, home_deltas_path, logs_delta_path, list(p_keys)


def main():
    # logs_delta_path = "/opt/notebooks/logs"
    # table_name = "md_account_d"
    # # p_keys = ["data_actual_date", "account_rk"]
    # p_keys = ["account_rk"]
    # home_deltas_path = "/opt/notebooks/data_deltas/"

    # Для вызова с параметрами из консоли
    num_args = len(sys.argv) - 1
    table_name, home_deltas_path, logs_delta_path, p_keys = argv_func(sys.argv[1], sys.argv[2], sys.argv[3], *sys.argv[4:num_args+1])
    print(table_name, home_deltas_path, logs_delta_path, p_keys)
    delta_loading(table_name, p_keys, home_deltas_path, logs_delta_path)




if __name__ == '__main__':
    main()
				
				
