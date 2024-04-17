import sys
import csv
import decimal
import psycopg2
import numpy as np
import pandas as pd
from datetime import datetime




def connect_insert(work_df):

    inicio_insert =  datetime.now()
    print(f"Inicio do insert: {inicio_insert}")

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
                curr.execute("SELECT * FROM tb_velocidade_teste LIMIT 5")
                #tup = curr.fetchall()
                curr.close()

                print("Conexao feita com sucesso")
            except (Exception, psycopg2.DatabaseError) as err:
                print(err)
                curr.close()
                return True            
        
        except (Exception, psycopg2.DatabaseError) as err:
            print(err)
            sys.exit()
        
        return conn

    conn = connect(param_dic)
    curr = conn.cursor()

    work_table = 'tb_velocidade'

    curr.execute("SELECT max(id) FROM "+work_table)
    tup = curr.fetchall()
    # df = pandas.DataFrame(tup)
    last_id = tup[0][0]

    data = work_df

    data.index += last_id+1

    data = data.reset_index()

    data.rename(columns={"index":"id"})

    data['contador'] = data['contador'].astype(int)
    data['tempo'] = data['tempo'].astype(int)
    print(data.head())

    data.columns = data.columns.str.replace('index', 'id')

    query = "INSERT INTO tb_velocidade (id,data_hora, placa_veiculo, velocidade_medida, latitude, longitude, numero_serie, bairro, regiao, cod_veiculo, desc_veiculo, contador, distancia, tempo, velocidade_media, hora_minuto) \
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

    values = [tuple(x) for x in data.to_numpy()]

    curr.executemany(query, values)

    conn.commit()

    fim_insert = datetime.now()
    total_insert = fim_insert - inicio_insert
    print(f"O processo de inserir demorou: {total_insert.total_seconds() / 60:.0f} minutos.")

    curr.close()
    conn.close()
    