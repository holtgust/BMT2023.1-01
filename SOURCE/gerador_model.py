import pandas as pd
from configparser import ConfigParser
import numpy as np
from utils import tf, idf
import logging
import time


class GerarModelo:
    def __init__(self):
        logging.info("Gerador do Modelo - Início")
        self.time_inicio = time.time()

        config = ConfigParser()
        config.read(r'.\config\INDEX.CFG')

        self.LEIA_LI = config.get("index", 'LEIA_LI')
        self.LEIA_N_D_T_MAX = config.get("index", 'LEIA_N_D_T_MAX')
        self.ESCREVA_MODELO = config.get("index", 'ESCREVA_MODELO')
        self.ESCREVA_W = config.get("index", 'ESCREVA_W')

        logging.info("Gerador do Modelo - Arquivos de configuração lidos")

        self.df_n_d_t_max = pd.read_csv(self.LEIA_N_D_T_MAX, sep=";", index_col=0)
        self.N_d = self.df_n_d_t_max.shape[0]

        self.df_modelo = pd.DataFrame([])
        self.df_W = pd.DataFrame([])

        self.run()
        self.export()

        logging.info("Gerador do Modelo - Modelo processado com sucesso em "
                     + str(time.time() - self.time_inicio) + "s")
    
    def tf_idf(self, row: pd.Series) -> dict:
        if type(row["RecordList"]) == str:
            record_list = eval(row["RecordList"])
        else:
            record_list = row["RecordList"]

        w = dict()
        for record_num, n_d_t in record_list.items():
            n_d_t_max = self.df_n_d_t_max.loc[record_num, "n_d_t_max"]
            w[record_num] = tf(n_d_t, n_d_t_max) * row["idf"]

        return w

    def run(self):
        df_modelo = pd.read_csv(self.LEIA_LI, sep=";")

        logging.info("Gerador do Modelo - Dados lidos em "
                     + str(time.time() - self.time_inicio) + "s")

        df_modelo["idf"] = df_modelo["n_d"].apply(lambda n_d: idf(n_d, self.N_d))
        df_modelo.drop("n_d", axis=1, inplace=True)

        df_modelo["w_d_t"] = df_modelo.apply(lambda row: self.tf_idf(row), axis=1)
        df_modelo.drop("RecordList", axis=1, inplace=True)

        dict_W = dict.fromkeys(self.df_n_d_t_max.index.values, 0)
        for idx, row in df_modelo.iterrows():
            for key in row["w_d_t"]:
                dict_W[key] += row["w_d_t"][key] ** 2

        df_W = pd.DataFrame({"RecordNum": dict_W.keys(), "W": dict_W.values()})
        df_W["W"] = np.sqrt(df_W["W"])

        self.df_modelo = df_modelo
        self.df_W = df_W

        logging.info("Gerador do Modelo - Foram lidos " + str(df_modelo.shape[0]+1) + " records")

    def export(self):
        # Exportar tabelas
        self.df_modelo.to_csv(self.ESCREVA_MODELO, index=False, sep=";")
        self.df_W.to_csv(self.ESCREVA_W, index=False, sep=";")


if __name__ == "__main__":
    GerarModelo()
