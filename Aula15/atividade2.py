import os

os.system('cls')
import polars as pl
# import numpy as np
import gc
from datetime import datetime

FONTE_DADOS = r'./dados/'

try:
    hora_import = datetime.now()   

    print('Obtendo dados...')

    inicio = datetime.now()
    
    list_arquivos = []

    list_dir_arquivos = os.listdir(FONTE_DADOS)

    for arquivo in list_dir_arquivos:
        if arquivo.endswith('.csv'):
            list_arquivos.append(arquivo)

    for arquivo in list_arquivos:
        print(f'Processando arquivo {arquivo}')

        df = pl.read_csv(FONTE_DADOS + arquivo, separator=';', encoding='iso-8859-1')

        if 'df_bolsa_familia' in locals():
            df_bolsa_familia = pl.concat([df_bolsa_familia, df])

        else:
            df_bolsa_familia = df

        df_bolsa_familia = df_bolsa_familia.with_columns(
            pl.col('VALOR PARCELA').str.replace(',', '.').cast(pl.Float64)
        )

        print(df_bolsa_familia)

        df_bolsa_familia.write_parquet(FONTE_DADOS + 'bolsa_familia_jan_fev.parquet')

        del df_bolsa_familia

        gc.collect()

        hora_impressao = datetime.now()

        print(f'Tempo de execução: {hora_impressao - hora_import}')

except ImportError as e:
    print('Erro ao obter dados: ', e)
