import glob
import pandas as pd

def data_append():
    path = "/home/uriel/Documents/ml_repo/data/raw"
    path_pattern = path + "/*.parquet.gzip"

    lista_paths = glob.glob(path_pattern)
    print(lista_paths)
    lista_dfs = []

    for file in lista_paths:
        df = pd.read_parquet(file)
        lista_dfs.append(df)

    df_concatenado = pd.concat(lista_dfs, ignore_index=True)
    path_to_save = "/home/uriel/Documents/ml_repo/data/processed/"
    df_concatenado.to_parquet(path_to_save+"df_concatenado.parquet.gzip", compression="gzip")

data_append()