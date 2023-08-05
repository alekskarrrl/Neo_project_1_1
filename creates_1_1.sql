-- Создадим схемы LOGS и DS
CREATE SCHEMA IF NOT EXISTS LOGS;
CREATE SCHEMA IF NOT EXISTS DS;


-- Создадим в схеме DS таблицы, которые будем наполнять данными
CREATE TABLE IF NOT EXISTS DS.FT_BALANCE_F (
	ON_DATE	date,
	ACCOUNT_RK	integer,
	CURRENCY_RK	integer,
	BALANCE_OUT NUMERIC(10, 2),
	CONSTRAINT ft_balance_f_pk PRIMARY KEY (ON_DATE, ACCOUNT_RK)
);



CREATE TABLE IF NOT EXISTS DS.FT_POSTING_F (
	OPER_DATE			date,
	CREDIT_ACCOUNT_RK	integer,
	DEBET_ACCOUNT_RK	integer,
	CREDIT_AMOUNT	NUMERIC(10, 2),
	DEBET_AMOUNT	NUMERIC(10, 2),
	CONSTRAINT ft_posting_f_pk PRIMARY KEY (OPER_DATE, CREDIT_ACCOUNT_RK, DEBET_ACCOUNT_RK)

);


CREATE TABLE IF NOT EXISTS DS.MD_ACCOUNT_D (
	DATA_ACTUAL_DATE		date,
	DATA_ACTUAL_END_DATE	date,
	ACCOUNT_RK				integer,
	ACCOUNT_NUMBER			char(20),
	CHAR_TYPE				char(1),
	CURRENCY_RK				integer,
	CURRENCY_CODE			char(3),
	CONSTRAINT md_account_d_pk PRIMARY KEY (DATA_ACTUAL_DATE, ACCOUNT_RK)

);


CREATE TABLE IF NOT EXISTS DS.MD_CURRENCY_D (
	CURRENCY_RK				integer,
	DATA_ACTUAL_DATE		date,
	DATA_ACTUAL_END_DATE	date,
	CURRENCY_CODE			char(3),
	CODE_ISO_CHAR			char(3),
	CONSTRAINT md_currency_d_pk PRIMARY KEY (CURRENCY_RK, DATA_ACTUAL_DATE)

);



CREATE TABLE IF NOT EXISTS DS.MD_EXCHANGE_RATE_D (
	DATA_ACTUAL_DATE		date,
	DATA_ACTUAL_END_DATE	date,
	CURRENCY_RK				integer,
	REDUCED_COURCE			NUMERIC(20, 8),
	CODE_ISO_NUM			char(3),
	CONSTRAINT md_exchange_rate_d_pk PRIMARY KEY (DATA_ACTUAL_DATE, CURRENCY_RK)
);



CREATE TABLE IF NOT EXISTS DS.MD_LEDGER_ACCOUNT_S(
	CHAPTER							char(1),
	CHAPTER_NAME						varchar(30),
	SECTION_NUMBER					integer,
	SECTION_NAME						varchar(30),
	SUBSECTION_NAME					varchar(30),
	LEDGER1_ACCOUNT					char(3),
	LEDGER1_ACCOUNT_NAME				varchar,
	LEDGER_ACCOUNT					char(5),
	LEDGER_ACCOUNT_NAME				varchar,
	CHARACTERISTIC					char(1),
	IS_RESIDENT						boolean,
	IS_RESERVE						boolean,
	IS_RESERVED						boolean,
	IS_LOAN							boolean,
	IS_RESERVED_ASSETS				boolean,
	IS_OVERDUE						boolean,
	IS_INTEREST						boolean,
	PAIR_ACCOUNT						char(5),
	START_DATE						date,
	END_DATE							date,
	IS_RUB_ONLY						boolean,
	MIN_TERM							NUMERIC(10, 2),
	MIN_TERM_MEASURE					NUMERIC(10, 2),
	MAX_TERM							NUMERIC(10, 2),
	MAX_TERM_MEASURE					NUMERIC(10, 2),
	LEDGER_ACC_FULL_NAME_TRANSLIT		text,
	IS_REVALUATION					boolean,
	IS_CORRECT						boolean,
	CONSTRAINT md_ledger_account_s_pk PRIMARY KEY (LEDGER_ACCOUNT, START_DATE)

);



-- В схеме LOGS создадим таблицы для записи логов
CREATE TABLE IF NOT EXISTS LOGS.LOG_LOADING (
	load_id					SERIAL,
	started_at				timestamp,
	finished_at				timestamp,
	CONSTRAINT log_loading_pk PRIMARY KEY (load_id)

);

CREATE TABLE IF NOT EXISTS LOGS.LOG_TABLES (
	event_time				timestamp,
	load_id					integer,
	source_table			varchar(30),
	event_type				varchar(20),
	rows_affected			integer,
	CONSTRAINT log_tables_pk PRIMARY KEY (event_time, source_table),
	CONSTRAINT loading_tables_fk FOREIGN KEY (load_id) REFERENCES LOGS.LOG_LOADING(load_id)

);

-- Запросы на просмотр логов
SELECT log_l.load_id, 
		started_at, 
		finished_at, 
		event_time, 
		source_table, 
		event_type, 
		rows_affected, 
		(event_time - started_at) as load_duration 
FROM LOGS.log_loading log_l LEFT JOIN LOGS.log_tables log_t ON log_l.load_id = log_t.load_id;


Select * From LOGS.log_loading;
Select * From LOGS.log_tables;

