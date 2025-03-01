/*
 Набор скриптов для создания схемы и основных таблиц в БД
 */

/*
 Создание схемы
 */
create schema if not exists "FRAUD_TRANSACTIONS";
set search_path to "FRAUD_TRANSACTIONS";


/*
 Создание стейджинговых таблиц
 */
create table if not exists "STG_TRANSACTIONS"(
	trans_id bigint,
	trans_date timestamp,
	card_num varchar(19),
	oper_type varchar(10) check(oper_type in ('PAYMENT', 'DEPOSIT', 'WITHDRAW')),
	amt decimal(16, 2),
	oper_result varchar(7) check(oper_result in ('SUCCESS', 'REJECT')),
	terminal varchar(5)
);

create table if not exists "STG_PASSPORT_BLACKLIST"(
	passport_num varchar(11),
	entry_dt date
);

create table if not exists "STG_TERMINALS"(
	terminal_id varchar(5) primary key,
	terminal_type varchar(3) check(terminal_type in ('ATM', 'POS')),
	terminal_city varchar(128),
	terminal_address varchar(128)
);


/*
 Создание таблиц измерений
 */
create table if not exists "DWH_DIM_TERMINALS_HIST"(
	terminal_id varchar(5),
	terminal_type varchar(3) check(terminal_type in ('ATM', 'POS')),
	terminal_city varchar(128),
	terminal_address varchar(128),
	effective_from timestamp default current_timestamp,
	effective_to timestamp default to_timestamp('2999-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS'),
	deleted_flg integer default 0,
	primary key (terminal_id, effective_from)
);

create table if not exists "DWH_DIM_CLIENTS_HIST"(
	client_id varchar(8) primary key,
	last_name varchar(128),
	first_name varchar(128),
	patrinymic varchar(128),
	date_of_birth date,
	passport_num varchar(11),
	passport_valid_to date,
	phone varchar(16),
	effective_from timestamp default current_timestamp,
	effective_to timestamp default to_timestamp('2999-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS'),
	deleted_flg integer default 0
);

create table if not exists "DWH_DIM_ACCOUNTS_HIST"(
	account_num varchar(20) primary key,
	valid_to date,
	client varchar(8) references "DWH_DIM_CLIENTS_HIST"(client_id),
	effective_from timestamp default current_timestamp,
	effective_to timestamp default to_timestamp('2999-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS'),
	deleted_flg integer default 0
);

create table if not exists "DWH_DIM_CARDS_HIST"(
	card_num varchar(19) primary key,
	account_num varchar(20) references "DWH_DIM_ACCOUNTS_HIST"(account_num),
	effective_from timestamp default current_timestamp,
	effective_to timestamp default to_timestamp('2999-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS'),
	deleted_flg integer default 0
);


/*
 Создание таблиц фактов
 */
create table if not exists "DWH_FACT_TRANSACTIONS"(
	trans_id bigint primary key,
	trans_date timestamp,
	card_num varchar(19) references "DWH_DIM_CARDS_HIST"(card_num),
	oper_type varchar(10) check(oper_type in ('PAYMENT', 'DEPOSIT', 'WITHDRAW')),
	amt decimal,
	oper_result varchar(7) check(oper_result in ('SUCCESS', 'REJECT')),
	terminal varchar(5)
);

create table if not exists "DWH_FACT_PASSPORT_BLACKLIST"(
	passport_num varchar(11) primary key,
	entry_dt date
);


/*
 Создание таблицы метаданных
 */
create table if not exists "META_DATA"(
	id serial primary key,
	table_name varchar(128) unique,
	last_update timestamp default to_timestamp('1900-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')
);