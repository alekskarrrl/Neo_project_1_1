CREATE SEQUENCE IF NOT EXISTS logs.seq_log_id;
--SELECT currval('logs.seq_log_id');
--SELECT nextval('logs.seq_log_id');

-- SEQUENCE dm.seq_lg_messages --
CREATE SEQUENCE IF NOT EXISTS dm.seq_lg_messages;

CREATE TABLE IF NOT EXISTS LOGS.LOG_INFO (
	record_id				integer,
	pipeline_run_id			integer,
	date_time				timestamptz,
	event_type				varchar, 
	message_				text,
	rows_affected			integer,
	pid						integer,
	message_type			text,
	usename					text, 
	datname					text, 
	client_addr				text, 
	application_name 		text,
	backend_start			text,
	info_					text
);
