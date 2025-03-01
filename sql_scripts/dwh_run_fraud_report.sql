/*
 Набор скриптов для создания отчета о мошеннических операциях REP_FRAUD
 */
set search_path to "FRAUD_TRANSACTIONS";


/*
 Создание таблицы-отчета REP_FRAUD
 */
create table if not exists "REP_FRAUD"(
	event_dt timestamp,
	passport varchar(11),
	fio varchar(128),
	phone varchar(16),
	event_code integer,
	event_type varchar(128),
	report_dt timestamp default current_timestamp
);


/*
 Создание временной таблицы STG_TMP_REP_FRAUD для сбора данных о мшеннических операциях в одной таблице
 */
create table "STG_TMP_REP_FRAUD" as
select 
	event_dt, 
	passport, 
	fio, 
	phone,
	event_code,
	event_type
from "REP_FRAUD";


/*
 1) Занесение во временную таблицу операций при заблокированном паспорте (тип 1)
 */
insert into "STG_TMP_REP_FRAUD" (event_dt, passport, fio, phone, event_code, event_type)
select
	t1.trans_date,
	t4.passport_num,
	concat_ws(' ', t4.last_name, t4.first_name, t4.patrinymic) as fio,
	t4.phone,
	1 as event_code,
	'Заблокированный паспорт' as event_type
from "DWH_FACT_TRANSACTIONS" t1
inner join "DWH_DIM_CARDS_HIST" t2
on t1.card_num = t2.card_num
inner join "DWH_DIM_ACCOUNTS_HIST" t3
on t2.account_num = t3.account_num
inner join "DWH_DIM_CLIENTS_HIST" t4
on t3.client = t4.client_id
inner join "DWH_FACT_PASSPORT_BLACKLIST" t5
on t4.passport_num = t5.passport_num
where t1.trans_date >= t5.entry_dt
  and t2.deleted_flg = 0
  and t2.effective_to = to_timestamp('2999-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS')
  and t3.deleted_flg = 0
  and t3.effective_to = to_timestamp('2999-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS')
  and t4.deleted_flg = 0
  and t4.effective_to = to_timestamp('2999-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS');


/*
2) Занесение во временную таблицу операций при просроченном паспорте (тип 2) 
(операции с просроченным паспортом специально вынесены в отдельный тип для более точной идентификации типа мошенничества)
*/
insert into "STG_TMP_REP_FRAUD" (event_dt, passport, fio, phone, event_code, event_type)
select
	t1.trans_date,
	t4.passport_num,
	concat_ws(' ', t4.last_name, t4.first_name, t4.patrinymic) as fio,
	t4.phone,
	2 as event_code,
	'Просроченный паспорт' as event_type
from "DWH_FACT_TRANSACTIONS" t1
inner join "DWH_DIM_CARDS_HIST" t2
on t1.card_num = t2.card_num
inner join "DWH_DIM_ACCOUNTS_HIST" t3
on t2.account_num = t3.account_num
inner join "DWH_DIM_CLIENTS_HIST" t4
on t3.client = t4.client_id
where t1.trans_date >= t4.passport_valid_to
  and t2.deleted_flg = 0
  and t2.effective_to = to_timestamp('2999-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS')
  and t3.deleted_flg = 0
  and t3.effective_to = to_timestamp('2999-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS')
  and t4.deleted_flg = 0
  and t4.effective_to = to_timestamp('2999-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS');


/*
3) Занесение во временную таблицу операций при недействительном договоре (тип 3) 
*/
insert into "STG_TMP_REP_FRAUD" (event_dt, passport, fio, phone, event_code, event_type)
select
	t1.trans_date,
	t4.passport_num,
	concat_ws(' ', t4.last_name, t4.first_name, t4.patrinymic) as fio,
	t4.phone,
	3 as event_code,
	'Недействительный договор' as event_type
from "DWH_FACT_TRANSACTIONS" t1
inner join "DWH_DIM_CARDS_HIST" t2
on t1.card_num = t2.card_num
inner join "DWH_DIM_ACCOUNTS_HIST" t3
on t2.account_num = t3.account_num
inner join "DWH_DIM_CLIENTS_HIST" t4
on t3.client = t4.client_id
where t1.trans_date >= t3.valid_to
  and t2.deleted_flg = 0
  and t2.effective_to = to_timestamp('2999-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS')
  and t3.deleted_flg = 0
  and t3.effective_to = to_timestamp('2999-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS')
  and t4.deleted_flg = 0
  and t4.effective_to = to_timestamp('2999-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS');


/*
4) Занесение во временную таблицу операций в разных городах в течение одного часа (тип 4) 
*/
insert into "STG_TMP_REP_FRAUD" (event_dt, passport, fio, phone, event_code, event_type)
select 
	min(next_trans_date) as trans_date,
	passport_num,
	fio,
	phone,
	4 as event_code,
	'Операции в разных городах' as event_type
from (
	select 
		passport_num,
		terminal_city,
		next_city,
		trans_date,
		next_trans_date,
		fio,
		phone	
	from (
		select
			t1.terminal_city,
			lead(terminal_city) over(partition by passport_num order by trans_date) as next_city,
			t2.trans_date,
			lead(trans_date) over(partition by passport_num order by trans_date) as next_trans_date,
			t5.passport_num,
			concat_ws(' ', t5.last_name, t5.first_name, t5.patrinymic) as fio,
			t5.phone
		from "DWH_DIM_TERMINALS_HIST" t1
		inner join "DWH_FACT_TRANSACTIONS" t2
		on t1.terminal_id = t2.terminal
		inner join "DWH_DIM_CARDS_HIST" t3
		on t2.card_num = t3.card_num
		inner join "DWH_DIM_ACCOUNTS_HIST" t4
		on t3.account_num = t4.account_num
		inner join "DWH_DIM_CLIENTS_HIST" t5
		on t4.client = t5.client_id
		where t1.deleted_flg = 0
		  and t1.effective_to = to_timestamp('2999-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS')
		  and t3.deleted_flg = 0
		  and t3.effective_to = to_timestamp('2999-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS')
		  and t4.deleted_flg = 0
		  and t4.effective_to = to_timestamp('2999-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS')
		  and t5.deleted_flg = 0
		  and t5.effective_to = to_timestamp('2999-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS')
	)
	where terminal_city <> next_city
	  and extract(hour from (next_trans_date - trans_date)) <= 1	
)
group by passport_num, fio, phone;


/*
5) Занесение во временную таблицу операций с подбором суммы (тип 5) 
*/
insert into "STG_TMP_REP_FRAUD" (event_dt, passport, fio, phone, event_code, event_type)
select 
	trans_date,
	passport_num,
	fio,
	phone,
	5 as event_code,
	'Подбор суммы' as event_type		
from (
	select 
		t4.passport_num,
		concat_ws(' ', t4.last_name, t4.first_name, t4.patrinymic) as fio,
		t4.phone,
		t1.card_num,
		lag(t1.amt, 3) over(partition by t1.card_num order by t1.trans_date) as prev_amt_3,
		lag(t1.amt, 2) over(partition by t1.card_num order by t1.trans_date) as prev_amt_2,
		lag(t1.amt) over(partition by t1.card_num order by t1.trans_date) as prev_amt_1,
		t1.amt,
		lag(t1.trans_date, 3) over(partition by t1.card_num order by t1.trans_date) as prev_trans_date_3,
		lag(t1.trans_date, 2) over(partition by t1.card_num order by t1.trans_date) as prev_trans_date_2,
		lag(t1.trans_date) over(partition by t1.card_num order by t1.trans_date) as prev_trans_date_1,
		t1.trans_date,
		lag(t1.oper_result, 3) over(partition by t1.card_num order by t1.trans_date) as prev_oper_result_3,
		lag(t1.oper_result, 2) over(partition by t1.card_num order by t1.trans_date) as prev_oper_result_2,
		lag(t1.oper_result) over(partition by t1.card_num order by t1.trans_date) as prev_oper_result_1,
		t1.oper_result,
		t1.oper_type
	from "DWH_FACT_TRANSACTIONS" t1
	inner join "DWH_DIM_CARDS_HIST" t2
	on t1.card_num = t2.card_num
	inner join "DWH_DIM_ACCOUNTS_HIST" t3
	on t2.account_num = t3.account_num
	inner join "DWH_DIM_CLIENTS_HIST" t4
	on t3.client = t4.client_id
	where t2.deleted_flg = 0
	  and t2.effective_to = to_timestamp('2999-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS')
	  and t3.deleted_flg = 0
	  and t3.effective_to = to_timestamp('2999-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS')
	  and t4.deleted_flg = 0
	  and t4.effective_to = to_timestamp('2999-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS')
)
where oper_type in ('WITHDRAW', 'PAYMENT')
  and prev_amt_3 is not null
  and prev_amt_2 is not null
  and prev_amt_1 is not null
  and prev_oper_result_3 = 'REJECT'
  and prev_oper_result_2 = 'REJECT'
  and prev_oper_result_1 = 'REJECT'
  and oper_result = 'SUCCESS'
  and prev_amt_3 > prev_amt_2
  and prev_amt_2 > prev_amt_1
  and prev_amt_1 > amt
  and extract(minute from (prev_trans_date_2 - prev_trans_date_3)) <= 20
  and extract(minute from (prev_trans_date_1 - prev_trans_date_2)) <= 20
  and extract(minute from (trans_date - prev_trans_date_1)) <= 20;


/*
Консолидированный перенос данных из временной таблицы в REP_FRAUD 
*/
insert into "REP_FRAUD" (event_dt, passport, fio, phone, event_code, event_type)
select
	t1.event_dt,
	t1.passport,
	t1.fio,
	t1.phone,
	t1.event_code,
	t1.event_type
from "STG_TMP_REP_FRAUD" t1
left join "REP_FRAUD" t2
on t1.event_dt = t2.event_dt
and t1.passport = t2.passport
and t1.fio = t2.fio
and t1.phone = t2.phone
and t1.event_code = t2.event_code
and t1.event_type = t2.event_type
where t2.passport is null;


/*
 Скрипт для обновления метаданных таблицы REP_FRAUD
 */
update "META_DATA"
set last_update = current_timestamp
where table_name = 'REP_FRAUD';


/*
Удаление временной таблицы STG_TMP_REP_FRAUD
*/
drop table if exists "STG_TMP_REP_FRAUD";