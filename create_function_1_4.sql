
CREATE OR REPLACE FUNCTION ds.min_max_posting(onDate date)
	returns table ("Category" text,
					"Entered_date" date,
				   credit_accnt_rk numeric,
				   debet_accnt_rk numeric,
				   credit_amnt_rub double precision, 
				   debet_amnt_rub double precision,
				   currency varchar(3)) 
Language plpgsql
AS $$

begin
	return query
	
	WITH t1 as (

	Select oper_date,
			credit_account_rk,
			debet_account_rk,
			credit_amount,
			debet_amount,
			COALESCE(ac.currency_rk, ac_.currency_rk) as currency_rk,
			COALESCE(ac.currency_code, ac_.currency_code) as currency_code

	FROM ds.ft_posting_f p
	LEFT JOIN ds.md_account_d ac ON p.debet_account_rk = ac.account_rk
	LEFT JOIN ds.md_account_d ac_ ON p.credit_account_rk = ac_.account_rk
	WHERE oper_date = onDate
	
	),
	t2 as (
			
		SELECT 		oper_date,
					credit_account_rk,
					debet_account_rk,
					credit_amount * COALESCE(er.reduced_cource, 1) as credit_amount_rub,
					debet_amount * COALESCE(er.reduced_cource, 1) as debet_amount_rub,
					t1.currency_rk as currency_rk,
					t1.currency_code as currency_code
		FROM t1
		left 
        join ds.md_exchange_rate_d er
          on er.currency_rk = t1.currency_rk
         and onDate between er.data_actual_date and er.data_actual_end_date
	)
	
	SELECT 'Min_credit_amount_rub',
			oper_date,
			credit_account_rk,
			debet_account_rk,
			credit_amount_rub,
			debet_amount_rub,
			currency_code
	FROM t2
	WHERE credit_amount_rub = (SELECT Min(credit_amount_rub) FROM t2)
	
	UNION ALL
	
	SELECT 'Max_credit_amount_rub',
			oper_date,
			credit_account_rk,
			debet_account_rk,
			credit_amount_rub,
			debet_amount_rub,
			currency_code
	FROM t2
	WHERE credit_amount_rub = (SELECT Max(credit_amount_rub) FROM t2)
	
	UNION ALL
	
	SELECT 'Min_debet_amount_rub',
			oper_date,
			credit_account_rk,
			debet_account_rk,
			credit_amount_rub,
			debet_amount_rub,
			currency_code
	FROM t2
	WHERE debet_amount_rub = (SELECT Min(debet_amount_rub) FROM t2)
	
	UNION ALL
	
	SELECT 'Max_debet_amount_rub',
			oper_date,
			credit_account_rk,
			debet_account_rk,
			credit_amount_rub,
			debet_amount_rub,
			currency_code
	FROM t2
	WHERE debet_amount_rub = (SELECT Max(debet_amount_rub) FROM t2);


end;$$


-- -- Вызываем с параметром
Select * from  ds.min_max_posting(to_date('2018-01-25','yyyy-mm-dd'));

