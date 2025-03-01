/*
 Набор скриптов для загрузки данных по терминалам в таблицу DWH_DIM_TERMINALS_HIST
 */
set search_path to "FRAUD_TRANSACTIONS";


/*
 1. Блок скриптов для выделения инкремента 
 */
/*
 1.1 Скрипты для создания представления STG_TERMINALS_V 
 */
drop view if exists "STG_TERMINALS_V";
create view "STG_TERMINALS_V" as
select 
	terminal_id,
	terminal_type,
	terminal_city,
	terminal_address
from "DWH_DIM_TERMINALS_HIST"
where current_timestamp between effective_from and effective_to
and deleted_flg = 0;


/*
 1.2 Скрипт для сбора новых строк в таблице STG_TMP_TERMINALS_NEW_ROWS 
 */
create table if not exists "STG_TMP_TERMINALS_NEW_ROWS" as
select
	t1.terminal_id,
	t1.terminal_type,
	t1.terminal_city,
	t1.terminal_address
from "STG_TERMINALS" t1
left join "STG_TERMINALS_V" t2
on t1.terminal_id = t2.terminal_id
where t2.terminal_id is null;


/*
 1.3 Скрипт для сбора удаленных строк в таблице STG_TMP_TERMINALS_DELETED_ROWS 
 */
create table if not exists "STG_TMP_TERMINALS_DELETED_ROWS" as
select 
	t1.terminal_id,
	t1.terminal_type,
	t1.terminal_city,
	t1.terminal_address
from "STG_TERMINALS_V" t1
left join "STG_TERMINALS" t2
on t1.terminal_id = t2.terminal_id
where t2.terminal_id is null;


/*
 1.4 Скрипт для сбора обновленных строк в таблице STG_TMP_TERMINALS_UPDATED_ROWS 
 */
create table if not exists "STG_TMP_TERMINALS_UPDATED_ROWS" as
select
	t1.terminal_id,
	t1.terminal_type,
	t1.terminal_city,
	t1.terminal_address
from "STG_TERMINALS" t1
inner join "STG_TERMINALS_V" t2
on t1.terminal_id = t2.terminal_id
and (t1.terminal_id <> t2.terminal_id or
	t1.terminal_type <> t2.terminal_type or
	t1.terminal_city <> t2.terminal_city or
	t1.terminal_address <> t2.terminal_address
);


/*
 2. Блок скриптов для загрузки инкремента в таблицу DWH_DIM_TERMINALS_HIST
 */
/*
 2.1 Скрипт для загрузки новых строк в таблицу DWH_DIM_TERMINALS_HIST
 */
insert into "DWH_DIM_TERMINALS_HIST"(
	terminal_id,
	terminal_type,
	terminal_city,
	terminal_address
) select
	terminal_id,
	terminal_type,
	terminal_city,
	terminal_address
from "STG_TMP_TERMINALS_NEW_ROWS";


/*
 2.2 Скрипты для загрузки обновленных строк в таблицу DWH_DIM_TERMINALS_HIST
 */
update "DWH_DIM_TERMINALS_HIST"
set effective_to = date_trunc('second', now() - interval '1 second')
where terminal_id in (
	select 
		terminal_id 
	from "STG_TMP_TERMINALS_UPDATED_ROWS"
)
and effective_to = to_timestamp('2999-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS');

insert into "DWH_DIM_TERMINALS_HIST"(
	terminal_id,
	terminal_type,
	terminal_city,
	terminal_address
) select
	terminal_id,
	terminal_type,
	terminal_city,
	terminal_address
from "STG_TMP_TERMINALS_UPDATED_ROWS";


/*
 2.3 Скрипты для загрузки удаленных строк в таблицу DWH_DIM_TERMINALS_HIST
 */
update "DWH_DIM_TERMINALS_HIST"
set effective_to = date_trunc('second', now() - interval '1 second')
where terminal_id in (
	select 
		terminal_id 
	from "STG_TMP_TERMINALS_DELETED_ROWS"
)
and effective_to = to_timestamp('2999-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS');

insert into "DWH_DIM_TERMINALS_HIST"(
	terminal_id,
	terminal_type,
	terminal_city,
	terminal_address,
	deleted_flg
) select
	terminal_id,
	terminal_type,
	terminal_city,
	terminal_address,
	1 as deleted_flg
from "STG_TMP_TERMINALS_DELETED_ROWS";


/*
 2.4 Скрипт для обновления метаданных таблицы DWH_DIM_TERMINALS_HIST
 */
update "META_DATA"
set last_update = current_timestamp
where table_name = 'DWH_DIM_TERMINALS_HIST';


/*
 2.5 Скрипты для для удаления временных таблиц с новыми, обновленными и удаленными строками:
 STG_TMP_TERMINALS_NEW_ROWS
 STG_TMP_TERMINALS_UPDATED_ROWS
 STG_TMP_TERMINALS_DELETED_ROWS
 */
drop table if exists "STG_TMP_TERMINALS_NEW_ROWS";
drop table if exists "STG_TMP_TERMINALS_UPDATED_ROWS";
drop table if exists "STG_TMP_TERMINALS_DELETED_ROWS";