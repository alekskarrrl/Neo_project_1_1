CREATE SCHEMA IF NOT EXISTS DM;

CREATE TABLE DM.DM_ACCOUNT_TURNOVER_F (
	on_date 				date,
	account_rk 				numeric,
	credit_amount 			numeric(23, 8),
	credit_amount_rub 		numeric(23, 8),
	debet_amount 			numeric(23, 8),
	debet_amount_rub 		numeric(23, 8)
--	CONSTRAINT DM_ACCOUNT_TURNOVER_F_PK PRIMARY KEY()

);

CREATE TABLE DM.DM_F101_ROUND_F (
	FROM_DATE 				date,
	TO_DATE					date,
	CHAPTER					char(1),
	LEDGER_ACCOUNT			char(5),
	CHARACTERISTIC			char(1),
	BALANCE_IN_RUB			numeric(23, 8),
	R_BALANCE_IN_RUB		numeric(23, 8),
	BALANCE_IN_VAL			numeric(23, 8),
	R_BALANCE_IN_VAL		numeric(23, 8),
	BALANCE_IN_TOTAL		numeric(23, 8),
	R_BALANCE_IN_TOTAL		numeric(23, 8),
	TURN_DEB_RUB			numeric(23, 8),
	R_TURN_DEB_RUB			numeric(23, 8),
	TURN_DEB_VAL			numeric(23, 8),
	R_TURN_DEB_VAL			numeric(23, 8),
	TURN_DEB_TOTAL			numeric(23, 8),
	R_TURN_DEB_TOTAL		numeric(23, 8),
	TURN_CRE_RUB			numeric(23, 8),
	R_TURN_CRE_RUB			numeric(23, 8),
	TURN_CRE_VAL			numeric(23, 8),
	R_TURN_CRE_VAL			numeric(23, 8),
	TURN_CRE_TOTAL			numeric(23, 8),
	R_TURN_CRE_TOTAL		numeric(23, 8),
	BALANCE_OUT_RUB			numeric(23, 8),
	R_BALANCE_OUT_RUB		numeric(23, 8),
	BALANCE_OUT_VAL			numeric(23, 8),
	R_BALANCE_OUT_VAL		numeric(23, 8),
	BALANCE_OUT_TOTAL		numeric(23, 8),
	R_BALANCE_OUT_TOTAL		numeric(23, 8)

);

-- Logging

-- SEQUENCE dm.seq_lg_messages --
CREATE SEQUENCE IF NOT EXISTS dm.seq_lg_messages;

CREATE TABLE dm.lg_messages ( 	
		record_id integer,
		date_time date,
		pid integer,
		message text,
		message_type text,
		usename text, 
		datname text, 
		client_addr text, 
		application_name text,
		backend_start date
)
