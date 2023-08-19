
create or replace procedure logs.write_log_info(
   i_message			varchar,
   i_messageType		int,
	pipeline_run_id 	bigint,
	event_type 			varchar,
	rows_affected 		integer,
	info_				text
)
language plpgsql    
as $$
declare
	log_NOTICE            constant int := 1;
	log_WARNING           constant int := 2;
	log_ERROR             constant int := 3;
	log_DEBUG             constant int := 4;

	c_splitToTable        constant int := 4000;
	c_splitToDbmsOutput   constant int := 900;

	v_logDate           timestamp;
	v_callerType        varchar;
	v_callerOwner       varchar;
	v_caller            varchar;
	v_line              numeric;
	v_message           varchar;
begin
    v_logDate := now();
    -- split to log table
    v_message := i_message;
	i_messageType	:= log_NOTICE;
	
    while length(v_message) > 0 loop
      insert into logs.log_info ( 	
							record_id,
							pipeline_run_id,
							date_time,
							event_type, 
							message_,
							rows_affected,
							pid,
							message_type,
							usename, 
							datname, 
							client_addr, 
							application_name,
							backend_start,
		  					info_
   					 )
	select 	
			nextval('dm.seq_lg_messages'),
			pipeline_run_id,
			now(),
			event_type,
			substr(v_message, 1, c_splitToTable),
			rows_affected,
			pid,
			i_messageType,
			usename, 
			datname, 
			client_addr, 
			application_name,
			backend_start,
			info_
	 from pg_stat_activity
	where pid = pg_backend_pid();
      v_message := substr(v_message, c_splitToTable + 1);
    end loop;

    commit;
end;$$