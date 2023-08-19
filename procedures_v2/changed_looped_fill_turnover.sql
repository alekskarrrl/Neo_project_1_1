--CALL dm.looped_fill_turnover(to_date('2018-01-31','yyyy-mm-dd'), 'some message')

create or replace procedure dm.looped_fill_turnover ( 
  date_to date,
  info_msg varchar
)
language plpgsql    
as $$
declare
	date_from  date = date(date_trunc('MONTH', date_to));
	onDay date = date_from;
	curr_seq_log_id bigint;
begin
	curr_seq_log_id := (SELECT last_value FROM logs.seq_log_id);
	call logs.write_log_info('[BEGIN LOOP] looped_fill_turnover(from_date => date ''' 
         || to_char(date_from, 'yyyy-mm-dd') || ''' to_date => date ''' || to_char(date_to, 'yyyy-mm-dd')
         || ''');', 1, curr_seq_log_id, 'call_procedure dm.looped_fill_turnover', Null, info_msg);
		 
	WHILE onDay <= date_to LOOP
    	call ds.fill_account_turnover_f(onDay, info_msg);
		onDay = onDay + 1;
		PERFORM pg_sleep(1);
	END LOOP;
	
	call logs.write_log_info('[END LOOP] looped_fill_turnover(from_date => date ''' 
         || to_char(date_from, 'yyyy-mm-dd') || ''' to_date => date ''' || to_char(date_to, 'yyyy-mm-dd')
         || ''');', 1, curr_seq_log_id, 'call_procedure dm.looped_fill_turnover', Null, info_msg);
	
end;$$