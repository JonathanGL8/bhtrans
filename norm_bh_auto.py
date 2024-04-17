import decimal
import psycopg2
import warnings
import numpy as np
import numpy as np
import pandas as pd
from datetime import datetime
from datetime import timedelta


warnings.simplefilter(action='ignore', category=FutureWarning)

conn = psycopg2.connect(
    host="localhost",
    database="db_bh",
    user="postgres",
    password="usr_bh"
)

# Cria um cursor
cur = conn.cursor()


def get_info_dist_gcp():
    sql = "SELECT * FROM distancias_gcp"

    cur.execute(sql)
    df_gcp = pd.DataFrame(cur.fetchall(), columns=['id',
                                                   'radarbase',
                                                   'latitudebase',
                                                   'longitudebase',
                                                   'radarcomparacao',
                                                   'latitudecomparacao',
                                                   'longitudecomparacao',
                                                   'enderecodestino',
                                                   'enderecoorigem',
                                                   'distanciatexto',
                                                   'distanciavalor',
                                                   'duracaotexto',
                                                   'duracaovalor'])

    conn.close()
    cur.close()

    return df_gcp


df_gcp = get_info_dist_gcp()
string_columns = ['latitudebase', 'longitudebase', 'latitudecomparacao', 'longitudecomparacao']
df_gcp[string_columns] = df_gcp[string_columns].astype(str)

def principal(df):

    inicio_transformacao = datetime.now()
    print(f"Inicio da transformacao: {inicio_transformacao}")

    df['Latitude'] = df['Latitude'].astype("string")
    df['Longitude'] = df['Longitude'].astype("string")

    df['DataHoraPassagem'] = pd.to_datetime(df['DataHoraPassagem'])

    df[df['PlacaVeiculo'] == ''] = np.NaN

    count_inicial_1 = df['DataHoraPassagem'].count()

    df = df.dropna(axis=0)

    count_limpeza_1 = df['DataHoraPassagem'].count()

    final = count_inicial_1 - count_limpeza_1
    print("Foram deletadas " + str(final) + " placas que estavam vazias de um total de "+str(count_inicial_1))

    df['Latitude'] = df['Latitude'].replace(",", ".", regex=True)
    df['Longitude'] = df['Longitude'].replace(",", ".", regex=True)

    df_silver = df[
        ['DataHoraPassagem', 'PlacaVeiculo', 'VelocidadeMedida', 'Latitude', 'Longitude', 'NumeroSerie', 'Bairro', 'Regiao',
        'cod_veiculo', 'desc_veiculo']].sort_values(by=['PlacaVeiculo']).drop_duplicates()
    PlacaPorRadar_silver = df_silver['PlacaVeiculo'].value_counts().rename_axis('PlacaVeiculo').reset_index(name="Contador")

    PlacaPorRadar_silver = PlacaPorRadar_silver[PlacaPorRadar_silver['Contador'] > 1].sort_values(by=['PlacaVeiculo'])

    df_silver[df_silver.index.isin(PlacaPorRadar_silver.index)].sort_values(by=['PlacaVeiculo'])

    df_silver = df_silver.merge(PlacaPorRadar_silver, on='PlacaVeiculo', how='right').sort_values(
        by=['PlacaVeiculo', 'DataHoraPassagem'])

    df_silver["Distancia"] = np.NaN
    df_silver["Tempo"] = np.NaN

    df_silver['Latitude'].astype(float)
    df_silver['Longitude'].astype(float)

    group = df_silver.groupby('PlacaVeiculo')
    time_diff = group['DataHoraPassagem'].diff().dt.total_seconds().abs()
    # time_diff['values'] = time_diff.groupby('category')['values'].bfill()
    df_silver['Tempo'] = time_diff
    df_silver['Tempo'] = df_silver.groupby('PlacaVeiculo')['Tempo'].bfill()

    df_silver['next_latitiude'] = df_silver.groupby('PlacaVeiculo')['Latitude'].shift(-1)
    df_silver['next_longitude'] = df_silver.groupby('PlacaVeiculo')['Longitude'].shift(-1)

    df_gcp1 = df_gcp[['latitudebase', 'longitudebase', 'latitudecomparacao', 'longitudecomparacao', 'distanciavalor']]

    df_silver = df_silver.merge(df_gcp1, how='left', left_on=['Latitude',
                                                            'Longitude', 'next_latitiude',
                                                            'next_longitude'],
                                right_on=['latitudebase', 'longitudebase', 'latitudecomparacao',
                                        'longitudecomparacao']).drop_duplicates()

    # get reverse distance
    df_silver = df_silver.merge(df_gcp1, how='left', left_on=['next_latitiude',
                                                            'next_longitude', 'Latitude',
                                                            'Longitude'],
                                right_on=['latitudebase', 'longitudebase', 'latitudecomparacao',
                                        'longitudecomparacao']).drop_duplicates()

    df_silver['distanciavalor_y'] = df_silver.groupby('PlacaVeiculo')['distanciavalor_y'].shift(1)

    df_silver['Distancia'] = df_silver.groupby('PlacaVeiculo')['Distancia'].fillna(df_silver['distanciavalor_x']).fillna(
        df_silver['distanciavalor_y'])

    del_columns = ['next_latitiude',
                'next_longitude', 'latitudebase_x', 'longitudebase_x',
                'latitudecomparacao_x', 'longitudecomparacao_x', 'distanciavalor_x',
                'latitudebase_y', 'longitudebase_y', 'latitudecomparacao_y',
                'longitudecomparacao_y', 'distanciavalor_y']

    df_silver.drop(del_columns, inplace=True, axis=1)

    df_silver.isna().sum()
    df_silver[df_silver['Distancia'] == 0] = np.NaN

    df_silver[df_silver['VelocidadeMedida'] > 400] = np.NaN

    num_missing_rows = df_silver.isnull().sum(axis=1).astype(bool).sum()
    print("Number of rows with missing values in df_silver:", num_missing_rows)

    df_silver = df_silver.dropna(axis=0)

    df_silver['Tempo'] = df_silver['Tempo'].astype(int)

    df_silver[df_silver['Distancia'] == 0]

    df_silver["VelocidadeMedia"] = np.NaN
    df_silver["HoraMinuto"] = np.NaN

    df_silver['Contador'] = df_silver['Contador'].astype(int)
    df_silver['Tempo'] = df_silver['Tempo'].astype(int)

    df_silver = df_silver.query('Distancia != 0 and Tempo != 0')
    df_silver['VelocidadeMedia'] = (df_silver['Distancia'].astype(float)  / df_silver['Tempo']) * 3.6
    df_silver['VelocidadeMedia'] = df_silver['VelocidadeMedia'].astype(float)

    df_silver['VelocidadeMedia'] = df_silver['VelocidadeMedia'].round(3)

    df_silver['HoraMinuto'] = df_silver['DataHoraPassagem'].dt.strftime('%H:%M')

    df_silver['Distancia'] = df_silver['Distancia'].apply(lambda x: decimal.Decimal(x/1000).quantize(decimal.Decimal('0.000')))

    df_union = df_silver.reset_index()
    df_union = df_silver.rename(columns={"index": "id", "DataHoraPassagem": "data_hora", "PlacaVeiculo": "placa_veiculo",
                                        "VelocidadeMedida": "velocidade_medida", "Latitude": "latitude",
                                        "Longitude": "longitude",
                                        "NumeroSerie": "numero_serie", "Bairro": "bairro", "Regiao": "regiao",
                                        "Contador": "contador", "Distancia": "distancia", "Tempo": "tempo",
                                        "VelocidadeMedia": "velocidade_media", "HoraMinuto": "hora_minuto"})

    df_union = df_union.reset_index()

    df_union = df_union.rename(columns={"index": "id"})

    df_union = df_union.drop_duplicates()

    df_union = df_union.dropna(axis=0)

    print("Finalizado com o total de registros:")
    print(df_union['id'].count())

    df_final = df_union.drop(['id'], axis=1)

    data_hora_min = df_final['data_hora'].min()
    data_hora_max = df_final['data_hora'].max()

    local_save = f"save_snapshots/{data_hora_min}_{data_hora_max}.csv"

    df_union.to_csv(local_save, sep=';',encoding='utf-8', index = False)

    fim_transformacao = datetime.now()
    total_transformacao = fim_transformacao - inicio_transformacao
    print(f"O processo demorou: {total_transformacao.total_seconds() / 60:.0f} minutos.")
    print("Finalizado")

    return df_final
