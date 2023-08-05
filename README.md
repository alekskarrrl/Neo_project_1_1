

<a name="readme-top"></a>
# Проектное задание: Data Engineering


## Навигация
* [Задание 1.1](#задание-11)    
  * [Демо 1.1](#демо-11)  
  * [Подготовка базы данных PostgreSQL](#подготовка-базы-данных-postgresql)  
  * [Airflow граф](#airflow-граф)  
* [Задание 1.2](#задание-12)
  * [Демо 1.2](#демо-12)  
  * [Дополнительные таблицы и процедуры в PostgreSQL](#дополнительные-таблицы-и-процедуры-в-postgresql)
  * [Airflow DAG](#airflow-dag)  
* [Инструменты](#инструменты)
* [Источники данных](#источники-данных)
* [Запуск контейнеров](#запуск-контейнеров)



<p align="right">(<a href="#readme-top">back to top</a>)</p>

## Задание 1.1
<details> 
  <summary>Задание 1.1 </summary>
Разработать ETL-процесс для загрузки «банковских» данных из csv-файлов в соответствующие таблицы 
СУБД Oracle или PostgreSQL. Покрыть данный процесс логированием этапов работы и всевозможной 
дополнительной статистикой (на усмотрение вашей фантазии). В исходных файлах могут быть ошибки в 
виде некорректных форматах значений. Но глядя на эти значения вам будет понятно, какие значения 
имеются в виду.


#### Исходные данные:  
Данные из 6 таблиц в виде excel-файлов:  
`md_ledger_account_s` – справочник балансовых счётов;  
`md_account_d` – информация о счетах клиентов;  
`ft_balance_f` – остатки средств на счетах;  
`ft_posting_f` – проводки (движения средств) по счетам;  
`md_currency_d` – справочник валют;  
`md_exchange_rate_d` – курсы валют.  

</details>


<p align="right">(<a href="#readme-top">back to top</a>)</p>   

### Демо 1.1

Ссылка на видеозапись демонстрации:  [Demo Video](https://drive.google.com/drive/folders/1WyyQH9z0u-7FtLvH-rslwsaq3C2sUj1b?usp=sharing)


<p align="right">(<a href="#readme-top">back to top</a>)</p>

### Подготовка базы данных PostgreSQL

В pgAdmin4 создать поодключение к базе данных:  
Name  `neo_db_pg`   
Host `neo_db_pg`  (или по имени сервиса: `postgres-db`)  
User / Pass -  `admin / admin`  

#### Создадим схемы и таблицы
Файл `creates_1_1.sql` в корневой папке проекта содержит необходимые инструкции.

- Создадим схемы LOGS и DS
- Создадим в схеме DS таблицы, которые будем наполнять данными
- В схеме LOGS создадим таблицы для записи логов

> При выполнении задания 1.2 были изменены типы полей в таблицах схемы DS для приведения в соответствие со структурой таблиц, представленной в задании.  
> Финальные инструкции для создания таблиц слоя DS - `create_1_1_changed_types.sql`.  
> Эта корректировка не повлияла на процесс загрузки данных из файлов csv в слой DS.   


<p align="right">(<a href="#readme-top">back to top</a>)</p>

### Airflow граф

Добавляем подключение к postgres  
Name  `neo_db_pg`  
Host `neo_db_pg`  (или по имени сервиса: `postgres-db`)  
Schema `neo_db_pg`  
User / Pass -  `admin / admin`

Код с описанием процесса на Python:
`airflow/dags/neo_dag_1_1.py`

Визуализация графа:

![Airflow DAG](img/dag_1_1.JPG "")


Логи после завершения процесса загрузки выглядят так:

![Logs result](img/logs.JPG "")


<p align="right">(<a href="#readme-top">back to top</a>)</p>


## Задание 1.2
<details> 
  <summary>Задание 1.2 </summary>

После того как детальный слой «DS» успешно наполнен исходными данными из файлов – нужно рассчитать витрины данных в слое «DM»: витрину оборотов и витрину 101-й отчётной формы.  

Для этого вам сперва необходимо построить витрину оборотов «DM.DM_ACCOUNT_TURNOVER_F». А именно, посчитать за каждый день января 2018 года кредитовые и дебетовые обороты по счетам с помощью Oracle-пакета dm.fill_account_turnover_f или с помощью аналогичной PostgreSQL-процедуры.

Затем вы узнаёте от Аналитика в банке, что пакет (или процедуру) расчёта витрины 101-й формы «dm.fill_f101_round_f» необходимо доработать. Необходимо сделать расчёт полей витрины «dm.dm_f101_round_f» по формулам:

`BALANCE_OUT_RUB`  
для счетов с CHARACTERISTIC = 'A' и currency_code '643' рассчитать   
`BALANCE_OUT_RUB = BALANCE_IN_RUB - TURN_CRE_RUB + TURN_DEB_RUB;`  

для счетов с CHARACTERISTIC = 'A' и currency_code '810' рассчитать   
`BALANCE_OUT_RUB = BALANCE_IN_RUB - TURN_CRE_RUB + TURN_DEB_RUB;`  

для счетов с CHARACTERISTIC = 'P' и currency_code '643' рассчитать   
`BALANCE_OUT_RUB = BALANCE_IN_RUB + TURN_CRE_RUB - TURN_DEB_RUB;`  

для счетов с CHARACTERISTIC = 'P' и currency_code '810' рассчитать   
`BALANCE_OUT_RUB = BALANCE_IN_RUB + TURN_CRE_RUB - TURN_DEB_RUB;`  

`BALANCE_OUT_VAL`  
для счетов с CHARACTERISTIC = 'A' и currency_code не '643' и не '810' рассчитать   
`BALANCE_OUT_VAL = BALANCE_IN_VAL - TURN_CRE_VAL + TURN_DEB_VAL;`  

для счетов с CHARACTERISTIC = 'P' и currency_code не '643' и не '810'  рассчитать   
`BALANCE_OUT_VAL = BALANCE_IN_VAL + TURN_CRE_VAL - TURN_DEB_VAL;`  

`BALANCE_OUT_TOTAL`  
рассчитать `BALANCE_OUT_TOTAL как BALANCE_OUT_VAL + BALANCE_OUT_RUB`  

Обратите внимание, что в предоставленных вам пакетах (процедурах) есть процедура логирования, под них нужно создать соответствующие таблицы или реализовать собственный процесс логирования расчёта витрин – это будет только плюсом.

</details>


<p align="right">(<a href="#readme-top">back to top</a>)</p>   

### Демо 1.2

Ссылка на видеозапись демонстрации:  [Demo Video](https://drive.google.com/drive/)

<p align="right">(<a href="#readme-top">back to top</a>)</p>  


### Дополнительные таблицы и процедуры в PostgreSQL

Файл `creates_1_2.sql` в корневой папке проекта содержит необходимые инструкции.   

- Создадим схему `DM`  
- Создадим таблицы `DM.DM_ACCOUNT_TURNOVER_F` и `DM.DM_F101_ROUND_F`  
- Создадим таблицу для логов `dm.lg_messages`  
- Создадим хранимые процедуры через интерфейс pgAdmin4 или из терминала.  
Код для процедур находится в папке `procedures`:  
  - `procedure_writeog.sql`  
  - `proc_fill_account_turnover_f.sql`  
  - `proc_looped_fill_turnover.sql`  
  - `proc_fill_f101_round_f.sql`  


<p align="right">(<a href="#readme-top">back to top</a>)</p>  

### Airflow DAG

Код с описанием процесса на Python:
`airflow/dags/neo_dag_1_2.py`  

Визуализация графа:

![Airflow DAG](img/dag_1_2.JPG "")

<p align="right">(<a href="#readme-top">back to top</a>)</p>  

## Инструменты

- Apache Airflow 2.6.2
- Python 3.7
- PostgreSQL 14
- pgAdmin4 7.4
- Docker compose


<p align="right">(<a href="#readme-top">back to top</a>)</p>

## Источники данных

Исходные данные в формате csv находятся в папке проекта `airflow/dags/neo_data`


<div><p align="right">(<a href="#readme-top">back to top</a>)</p></div>


## Запуск контейнеров

Для запуска postgres и pgAdmin просто выполнить команду  
`docker compose up -d`  
находясь в папке `postgres` или указать путь к файлу docker-compose.yaml с помощью опции -f


Для Airflow    
Файл `docker-compose.yaml` взят из документации [здесь](https://airflow.apache.org/docs/apache-airflow/2.6.2/howto/docker-compose/index.html#fetching-docker-compose-yaml)   
Изменен только порт для веб-сервера и дописан доступ воркеру во внешнюю сеть, чтобы обеспечить доспуп к нашей базе postgresql.  
Инструкции по запуску контейнеров с сервисами Airflow так же можно найти в документации по ссылке выше.


<p align="right">(<a href="#readme-top">back to top</a>)</p>

