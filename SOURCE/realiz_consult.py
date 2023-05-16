import pandas as pd
from configparser import ConfigParser
from utils import tf, count_in_list
import logging
import time


class RealizarConsultas:
    def __init__(self):
        logging.info("Realizar Consultas - Início")
        self.time_inicio = time.time()

        config = ConfigParser()
        config.read(r'.\config\BUSCA.CFG')

        self.MODELO = config.get("busca", 'MODELO')
        self.MODELO_W = config.get("busca", 'MODELO_W')
        self.CONSULTAS = config.get("busca", 'CONSULTAS')
        self.RESULTADOS = config.get("busca", 'RESULTADOS')

        logging.info("Realizar Consultas - Arquivos de configuração lidos")

        self.df_scores_all = pd.DataFrame([])

        self.run()
        self.export()

        logging.info("Realizar Consultas - Consultas processados com sucesso em "
                     + str(time.time() - self.time_inicio) + "s")

    def run(self):
        df_modelo = pd.read_csv(self.MODELO, sep=";", index_col=0)
        df_W = pd.read_csv(self.MODELO_W, sep=";", index_col=0)

        df_consultas = pd.read_csv(self.CONSULTAS, sep=";")

        logging.info("Realizar Consultas - Dados lidos em "
                     + str(time.time() - self.time_inicio) + "s")

        ls_scores_all = []
        for _, row in df_consultas.iterrows():
            query_number = row["QueryNumber"]
            query_text = row["QueryText"]

            query_text = query_text.split()
            query_text = [txt for txt in query_text if len(txt) >= 2]

            count_termos = count_in_list(query_text)

            n_q_t_max = max(count_termos.values())

            acumulador = dict()
            for termo, n_q_t in count_termos.items():
                if termo not in df_modelo.index:
                    continue

                row_modelo = df_modelo.loc[termo]
                modelo_doc_weights = eval(row_modelo["w_d_t"])

                tf_local = tf(n_q_t, n_q_t_max)
                idf_local = row_modelo["idf"]

                w_q_t = tf_local * idf_local

                for record_num in modelo_doc_weights:
                    if record_num in acumulador:
                        acumulador[record_num] += modelo_doc_weights[record_num] * w_q_t
                    else:
                        acumulador[record_num] = modelo_doc_weights[record_num] * w_q_t

            ls_record_num = []
            ls_score = []
            for record_num in acumulador:
                ls_record_num.append(record_num)
                ls_score.append(acumulador[record_num]/df_W.loc[record_num, "W"])

            df_scores = pd.DataFrame({"RecordNum": ls_record_num, "Score": ls_score})
            df_scores = df_scores.sort_values("Score", ascending=False)
            df_scores["Ranking"] = range(1, df_scores.shape[0]+1)
            df_scores["QueryNumber"] = query_number
            df_scores = df_scores[["QueryNumber", "Ranking", "RecordNum", "Score"]]

            ls_scores_all.append(df_scores)

        df_scores_all = pd.concat(ls_scores_all)

        self.df_scores_all = df_scores_all

        logging.info("Gerador do Modelo - Foram lidos " + str(df_consultas.shape[0]+1) + " records")

    def export(self):
        self.df_scores_all.to_csv(self.RESULTADOS, index=False, sep=";")


if __name__ == "__main__":
    RealizarConsultas()
