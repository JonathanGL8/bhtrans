from dotenv import load_dotenv
import os
import pyodbc
from datetime import datetime
load_dotenv()

inicio_tempo =  datetime.now()
print(f"Inicio do processamento: {inicio_tempo}")

db_server = os.getenv('server')
db_name = os.getenv('database')
db_user = os.getenv('username')
db_password = os.getenv('password')

driver= '{ODBC Driver 17 for SQL Server}' # Dependendo da versão do driver, pode variar o nome

# class MySQL:
#     def __init__(self):
#         self.conn = None

#     def __enter__(self):
#         self.conn = pyodbc.connect(f'DRIVER={driver};SERVER={db_server};DATABASE={db_name};UID={db_user};PWD={db_password}')
#         self.conn.autocommit = True

#     def __exit__(self, *args):
#         if self.conn:
#             self.conn.close()
#             self.conn = None


def query_upload_main():
    print(db_server)
    try:
        print("Entrou aqui")
        cnxn = pyodbc.connect(f'DRIVER={driver};SERVER={db_server};DATABASE={db_name};UID={db_user};PWD={db_password}')
        print("Conexao com banco feita com sucesso!")
    except pyodbc.Error as e:
        print(f"Ocorreu um erro durante a conexão com o banco de dados: {e}")    # mysql = MySQL()


    # with mysql:
    #     print("Teste conexao")


query_upload_main()