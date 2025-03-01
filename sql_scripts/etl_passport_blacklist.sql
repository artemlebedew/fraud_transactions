/*
 Скрипт для загрузки новых данных в DWH_FACT_PASSPORT_BLACKLIST
 */
set search_path to "FRAUD_TRANSACTIONS";


insert into "DWH_FACT_PASSPORT_BLACKLIST"(
	passport_num, 
	entry_dt
) select
	passport_num, 
	entry_dt
from "STG_PASSPORT_BLACKLIST"
where passport_num not in (
	select 
		passport_num
	from "DWH_FACT_PASSPORT_BLACKLIST"
);


/*
 Скрипт для обновления метаданных таблицы DWH_FACT_PASSPORT_BLACKLIST
 */
update "META_DATA"
set last_update = current_timestamp
where table_name = 'DWH_FACT_PASSPORT_BLACKLIST';