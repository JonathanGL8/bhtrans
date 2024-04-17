from dotenv import load_dotenv
import os
import pyodbc
import pandas as pd
from norm_bh_auto import principal
import insert_bh
from datetime import datetime
import query_main
import time
 
load_dotenv()

inicio_tempo =  datetime.now()
print(f"Inicio do processamento: {inicio_tempo}")

db_server = os.getenv('server')
db_name = os.getenv('database')
db_user = os.getenv('username')
db_password = os.getenv('password')

driver= '{ODBC Driver 17 for SQL Server}' # Dependendo da versão do driver, pode variar o nome


class MySQL:
    def __init__(self):
        self.conn = None

    def __enter__(self):
        self.conn = pyodbc.connect(f'DRIVER={driver};SERVER={db_server};DATABASE={db_name};UID={db_user};PWD={db_password}')
        self.conn.autocommit = True

    def __exit__(self, *args):
        if self.conn:
            self.conn.close()
            self.conn = None


def query_upload_main(query):
    mysql = MySQL()
    with mysql:
        start_query = datetime.now()
        print(f"Inicio da busca no SQL server: {start_query}")
        df = pd.read_sql(query, mysql.conn)
        #df = pd.read_sql(query, cnxn)
        fim_query = datetime.now()
        total_query = fim_query - start_query
        print(f"O processo de busca demorou: {total_query.total_seconds() / 60:.0f} minutos.")
        df_norm_final = norm_bh_auto.principal(df)
        insert_bh.connect_insert(df_norm_final)
        print(f"Finalizado query com where {query}")

    time.sleep(20)


# try:
#     cnxn = pyodbc.connect(f'DRIVER={driver};SERVER={db_server};DATABASE={db_name};UID={db_user};PWD={db_password}')
#     print("Conexao com banco feita com sucesso!")
# except pyodbc.Error as e:
#     print(f"Ocorreu um erro durante a conexão com o banco de dados: {e}")

def main():
    print("entrou na main")
    list_querys = query_main.get_query_range()
    #print(list_querys)
    for x in list_querys:
        print(x)
        print(type(x))
        
        query = f"""
                    SELECT 
                ps.DataHoraPassagem,
                ps.PlacaVeiculo,
                ps.VelocidadeMedida,
                ps.VelocidadeConsiderada,
                ls.Latitude,
                ls.Longitude,
                eq.NumeroSerie,
                fx.Bairro,
                rg.Nome as Regiao,
                tc.Codigo as cod_veiculo,
                tc.Descricao as desc_veiculo
            FROM dbo.TBPassagens ps 
            LEFT JOIN dbo.TBEquipamentos eq ON eq.Id = ps.Equipamento_id
            LEFT JOIN dbo.TBFaixas fx ON fx.Id = ps.Faixa_id
            LEFT JOIN dbo.TBLocais ls ON fx.Local_id = ls.Id
            LEFT JOIN dbo.TBRegioes rg ON ls.Regiao_id = rg.id
            LEFT JOIN dbo.TBClassificacoesVeiculos as tc on tc.Id = ps.ClassificacaoVeiculo_id
            {x}
            AND ps.PlacaVeiculo IS NOT NULL 
            AND ps.PlacaVeiculo <> ''
                """
        
        query_upload_main(query)

        #WHERE ps.DataHoraPassagem >= '2023-03-10 12:00:01' and ps.DataHoraPassagem <= '2023-03-11 00:00:00'

        


    # with pyodbc.connect(f'DRIVER={driver};SERVER={db_server};DATABASE={db_name};UID={db_user};PWD={db_password}') as connection:
    #     print("Conexao com banco feita com sucesso!")
    #     list_querys = query_main.get_query_range()

    #     for x in list_querys:
    #         query = f"""
    #                     SELECT 
    #                 ps.DataHoraPassagem,
    #                 ps.PlacaVeiculo,
    #                 ps.VelocidadeMedida,
    #                 ps.VelocidadeConsiderada,
    #                 ls.Latitude,
    #                 ls.Longitude,
    #                 eq.NumeroSerie,
    #                 fx.Bairro,
    #                 rg.Nome as Regiao,
    #                 tc.Codigo as cod_veiculo,
    #                 tc.Descricao as desc_veiculo
    #             FROM dbo.TBPassagens ps 
    #             LEFT JOIN dbo.TBEquipamentos eq ON eq.Id = ps.Equipamento_id
    #             LEFT JOIN dbo.TBFaixas fx ON fx.Id = ps.Faixa_id
    #             LEFT JOIN dbo.TBLocais ls ON fx.Local_id = ls.Id
    #             LEFT JOIN dbo.TBRegioes rg ON ls.Regiao_id = rg.id
    #             LEFT JOIN dbo.TBClassificacoesVeiculos as tc on tc.Id = ps.ClassificacaoVeiculo_id
    #             {x}
    #             AND ps.PlacaVeiculo IS NOT NULL 
    #             AND ps.PlacaVeiculo <> ''
    #                 """

    #         #WHERE ps.DataHoraPassagem >= '2023-03-10 12:00:01' and ps.DataHoraPassagem <= '2023-03-11 00:00:00'

    #         with connection.cursor() as cursor:
    #             df = pd.read_sql(query, cursor.connection)
    #             df_norm_final = norm_bh_auto.principal(df)
    #             insert_bh.connect_insert(df_norm_final)
    #             print(f"Finalizado query com where {x}")

    #         time.sleep(60)



    fim = datetime.now()
    hora_final= fim - inicio_tempo
    print(f"O processo completo demorou: {hora_final.total_seconds() / 60:.0f} minutos.")


    print("Todos os dados foram inseridos")


if __name__ == "__main__":
    main()
