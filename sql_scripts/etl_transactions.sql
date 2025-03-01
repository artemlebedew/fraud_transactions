/*
 Скрипт для загрузки новых данных в DWH_FACT_TRANSACTIONS
 */
set search_path to "FRAUD_TRANSACTIONS";


insert into "DWH_FACT_TRANSACTIONS"(
	trans_id, 
	trans_date, 
	card_num, 
	oper_type, 
	amt, 
	oper_result, 
	terminal
) select
	trans_id, 
	trans_date, 
	card_num, 
	oper_type, 
	amt, 
	oper_result, 
	terminal
from "STG_TRANSACTIONS";


/*
 Скрипт для обновления метаданных таблицы DWH_FACT_TRANSACTIONS
 */
update "META_DATA"
set last_update = current_timestamp
where table_name = 'DWH_FACT_TRANSACTIONS';