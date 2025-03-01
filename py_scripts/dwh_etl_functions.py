"""
Файл с функциями для создания таблиц БД и выполнения ETL-процесса по обработке 
и загрузке данных из источников:
1) transactions_DDMMYYYY.txt
2) terminals_DDMMYYYY.xlsx
3) passport_blacklist_DDMMYYYY.xlsx
"""

"""
0) Импорт модулей
"""
import psycopg2
import json
import pandas as pd
from sqlalchemy import create_engine
import os
import shutil


"""
1) Функция для подключения к БД
"""
def db_connect():
	with open("db_config.json", encoding="UTF-8") as f:
		db_config = json.load(f)

	sql_conn = f"postgresql://{db_config["user"]}:{db_config["password"]}@{db_config["host"]}:{db_config["port"]}/{db_config["database"]}"
	sql_engine = create_engine(sql_conn)

	conn = psycopg2.connect(**db_config)
	cursor = conn.cursor()
	cursor.execute('set search_path to "FRAUD_TRANSACTIONS";')

	return conn, cursor, sql_engine


"""
2) Функция для выполнения sql-скриптов
"""
def run_sql(script_path):
	conn, cursor, sql_engine = db_connect()

	with open(script_path, encoding="UTF-8") as f:
		sql_script = f.read()
		sql_scripts = sql_script.split(";")
	
	for script in sql_scripts:
		if script.strip():
			cursor.execute(script)
			conn.commit()
	conn.close()


"""
3) Функция для первоначальной загрузки данных из источников в стейджинговые таблицы
"""
def load_to_database(path, **sql_config):
	conn, cursor, sql_engine = db_connect()

	if path.startswith("transactions_"):
		cursor.execute('TRUNCATE TABLE "STG_TRANSACTIONS";')
		conn.commit()
		df = pd.read_csv(path, sep=";")
		df = df[["transaction_id", "transaction_date", "card_num", "oper_type", "amount", "oper_result", "terminal"]]
		df = df.rename(columns={"transaction_id": "trans_id", "transaction_date": "trans_date", "amount": "amt"})
		df["amt"] = df["amt"].str.replace(",", ".").astype(float)
		df.to_sql(con=sql_engine, **sql_config)
	elif path.startswith("terminals_"):
		cursor.execute('TRUNCATE TABLE "STG_TERMINALS";')
		conn.commit()
		df = pd.read_excel(path)
		df.to_sql(con=sql_engine, **sql_config)
	elif path.startswith("passport_blacklist_"):		
		cursor.execute('TRUNCATE TABLE "STG_PASSPORT_BLACKLIST";')
		conn.commit()
		df = pd.read_excel(path)
		df = df[["passport", "date"]]
		df = df.rename(columns={"passport": "passport_num", "date": "entry_dt"})
		df.to_sql(con=sql_engine, **sql_config)


"""
4) Функция для обработки и загрузки файлов на основе функции load_to_database
"""
def process_files():
	files_to_upload = [file for file in os.listdir(".") if os.path.isfile(file)]

	count = 0

	for file in files_to_upload:
		if file.startswith("terminals_") and file.endswith(".xlsx"):
			load_to_database(
				file,
				name="STG_TERMINALS", 
				schema="FRAUD_TRANSACTIONS", 
				if_exists="append", 
				index=False
			)
			run_sql("sql_scripts/etl_terminals.sql")
			shutil.move(file, f"archive/{file}.backup")
			print(f"Данные файла {file} загружены.")
			count += 1
		elif file.startswith("transactions_") and file.endswith(".txt"):
			load_to_database(
				file,
				name="STG_TRANSACTIONS", 			
				schema="FRAUD_TRANSACTIONS", 
				if_exists="append", 
				index=False
			)
			run_sql("sql_scripts/etl_transactions.sql")
			shutil.move(file, f"archive/{file}.backup")
			print(f"Данные файла {file} загружены.")
			count += 1
		elif file.startswith("passport_blacklist_") and file.endswith(".xlsx"):
			load_to_database(
				file, 
				name="STG_PASSPORT_BLACKLIST", 			
				schema="FRAUD_TRANSACTIONS", 
				if_exists="append", 
				index=False
			)
			run_sql("sql_scripts/etl_passport_blacklist.sql")		
			shutil.move(file, f"archive/{file}.backup")
			print(f"Данные файла {file} загружены.")
			count += 1

	return count