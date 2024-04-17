import pandas as pd
import sys
import psycopg2
import csv
import numpy as np
import datetime


param_dic = {
    "host": "localhost",
    "database": "db_bh",
    "user": "postgres",
    "password": "usr_bh"
}


def connect(param):
    conn = None

    try:
        conn = psycopg2.connect(**param)
        curr = conn.cursor()
        try: 
            curr.execute( """ select count(*) from distancias_gcp """)
            tup = curr.fetchall()
            print(tup)
            #curr.close()

            print("Conexao feita com sucesso")
        except (Exception, psycopg2.DatabaseError) as err:
            print(err)
            curr.close()
            return True            
    
    except (Exception, psycopg2.DatabaseError) as err:
        print(err)
        sys.exit()
    
    return conn


def get_query_range():
    
    conn = connect(param_dic)
    curr = conn.cursor()

    curr.execute("SELECT data_hora FROM master_tb_velocidade")
    df_data_hora = pd.DataFrame(curr.fetchall())
    df_data_hora.columns = ['data_hora']
    df_data_hora['data_hora'] = pd.to_datetime(df_data_hora['data_hora']).dt.strftime('%Y-%m-%d')
    list_days_in_postgres = df_data_hora['data_hora'].unique().tolist()
    print(list_days_in_postgres)

    start_date = datetime.datetime(2023, 6, 1, 0, 0, 0)
    end_date = datetime.datetime(2023, 6, 2, 23, 59, 59)

    # lista de todas as datas entre a data inicial e final, com intervalo de 1 dia
    date_list = [start_date + datetime.timedelta(days=x) for x in range(0, (end_date - start_date).days + 1)]

    # filtra as datas que já existem na lista existente
    new_dates = [date.strftime('%Y-%m-%d %H:%M:%S') for date in date_list if date.strftime('%Y-%m-%d') not in list_days_in_postgres]

    curr.close()
    conn.close()

    list_querys=[]

    # constrói a cláusula WHERE para cada dia
    for i in range(len(new_dates)):
        where_clause = " WHERE ps.DataHoraPassagem >= '{}' AND ps.DataHoraPassagem <= '{}' ".format(
            datetime.datetime.strptime(new_dates[i], '%Y-%m-%d %H:%M:%S').replace(hour=0, minute=0, second=0).strftime('%Y-%m-%d %H:%M:%S'),
            datetime.datetime.strptime(new_dates[i], '%Y-%m-%d %H:%M:%S').replace(hour=23, minute=59, second=59).strftime('%Y-%m-%d %H:%M:%S')
        )
        list_querys.append(where_clause)

    print(list_querys)
    return list_querys

def get_query_12_hours():
    
    conn = connect(param_dic)
    curr = conn.cursor()

    list_days_in_postgres = []

    curr.execute("SELECT DISTINCT data_hora::date FROM tb_velocidade")
    tup = curr.fetchall()
    for row in tup:
        list_days_in_postgres.append(row[0].strftime("%Y-%m-%d"))

    start_date = datetime.datetime(2023, 2, 1, 0, 0, 0)
    end_date = datetime.datetime(2023, 2, 28, 23, 59, 59)

    # lista de todas as datas entre a data inicial e final, com intervalo de 12 horas
    date_list = [start_date + datetime.timedelta(hours=x) for x in range(0, int((end_date - start_date).total_seconds()/3600) + 1, 12)]

    # filtra as datas que já existem na lista existente
    new_dates = [date.strftime('%Y-%m-%d %H:%M:%S') for date in date_list if date.strftime('%Y-%m-%d') not in list_days_in_postgres]

    curr.close()
    conn.close()

    list_querys=[]

    # constrói a cláusula WHERE para cada período de uma hora
    for i in range(len(new_dates)):
        if i == 0:
            # primeiro período
            where_clause = " WHERE ps.DataHoraPassagem >= '{}' AND ps.DataHoraPassagem < '{}' ".format(new_dates[i], (datetime.datetime.strptime(new_dates[i], '%Y-%m-%d %H:%M:%S') + datetime.timedelta(hours=12)).strftime('%Y-%m-%d %H:%M:%S'))
        else:
            # períodos subsequentes
            where_clause = " WHERE ps.DataHoraPassagem >= '{}' AND ps.DataHoraPassagem < '{}' ".format(new_dates[i], (datetime.datetime.strptime(new_dates[i], '%Y-%m-%d %H:%M:%S') + datetime.timedelta(hours=12)).strftime('%Y-%m-%d %H:%M:%S'))
        

        list_querys.append(where_clause)
        
    return list_querys
