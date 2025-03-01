"""
Главный скрипт управления процессом ETL
"""

"""
0) Импортирование модулей
"""
import psycopg2
import json
import pandas as pd
from sqlalchemy import create_engine
import os
import shutil
from py_scripts.dwh_etl_functions import db_connect, run_sql, process_files


"""
1) Настройка соединений
"""
try:
	print("Шаг 1: Установка соединения...")
	conn, cursor, sql_engine = db_connect()
	print("Шаг 1: Соединение установлено.", "------------------------------", sep="\n")	
except Exception as e:
	print("Соединение не установлено из-за ошибки в файле конфигурации.")


"""
2) Создание схемы FRAUD_TRANSACTIONS и вложенных таблиц.
"""
try:
	print("Шаг 2: Проверка наличия схемы и таблиц...")
	run_sql("sql_scripts/dwh_create_tables.sql")
	print("Шаг 2: Проверка завершена.", "------------------------------", sep="\n")	
except Exception as e:
	print(e)


"""
3) Заполнение таблиц исходными данными клиентов, карт и аккаунтов:
	a) DWH_DIM_CLIENTS_HIST
	b) DWH_DIM_ACCOUNTS_HIST
	c) DWH_DIM_CARDS_HIST
(Если исходные данные уже загружены - шаг пропускается с соответствующим сообщением в терминале.)
"""
try:
	print("Шаг 3: Загрузка исходных данных в таблицы DWH_DIM_CLIENTS_HIST, DWH_DIM_ACCOUNTS_HIST, DWH_DIM_CARDS_HIST...")
	run_sql("sql_scripts/dwh_insert_input_data.sql")
	print("Шаг 3: Исходные данные загружены", "------------------------------", sep="\n")
except psycopg2.errors.UniqueViolation as e:
	print("Шаг 3: Исходные данные уже загружены. Шаг загрузки пропускается.", "------------------------------", sep="\n")


"""
4) Чтение, обработка и загрузка данных терминалов, транзакций и паспортов в БД
	a) transactions_DDMMYYYY.txt в DWH_FACT_TRANSACTIONS
	b) terminals_DDMMYYYY.xlsx в DWH_DIM_TERMINALS_HIST
	c) passport_blacklist_DDMMYYYY.xlsx в DWH_FACT_PASSPORT_BLACKLIST
"""
try:
	print("Шаг 4: Загрузка данных из файлов-источников...")
	count = process_files()
	print(f"Шаг 4: Загрузка завершена. Всего обработан(о) {count} файл(ов).", "------------------------------", sep="\n")
except Exception as e:
	print(e)


"""
5) Формирование отчета по мошенническим операциям REP_FRAUD
"""
try:
	print("Шаг 5: Формирование отчета REP_FRAUD...")
	run_sql("sql_scripts/dwh_run_fraud_report.sql")
	print(f"Шаг 5: Отчет сформирован.")
except Exception as e:
	print(e)