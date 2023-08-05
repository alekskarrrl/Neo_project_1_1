--CALL dm.looped_fill_turnover(to_date('2018-01-31','yyyy-mm-dd'))

create or replace procedure dm.looped_fill_turnover ( 
  date_to date
)
language plpgsql    
as $$
declare
	date_from  date = date(date_trunc('MONTH', date_to));
	onDay date = date_from;
begin
	WHILE onDay <= date_to LOOP
    	call ds.fill_account_turnover_f(onDay);
		onDay = onDay + 1;
	END LOOP;
	
end;$$