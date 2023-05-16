from processar_consultas import ProcessadorConsultas
from gerar_lista_invertida import GeradorListaInvertida
from gerar_modelo import GerarModelo
from realizar_consultas import RealizarConsultas
import logging
import datetime


def processar_tudo():
    logging.basicConfig(filename=r".\result\LOG.log", encoding='utf-8', level=logging.DEBUG)

    logging.info("Início das operações - Datetime: " + str(datetime.datetime.now()))

    try:
        ProcessadorConsultas()
    except Exception as e:
        logging.error("Erro ao processar consultas: " + str(e))
        exit()
    try:
        GeradorListaInvertida()
    except Exception as e:
        logging.error("Erro ao gerar lista invertida: " + str(e))
        exit()
    try:
        GerarModelo()
    except Exception as e:
        logging.error("Erro ao gerar modelo: " + str(e))
        exit()
    try:
        RealizarConsultas()
    except Exception as e:
        logging.error("Erro ao realizar consultas: " + str(e))
        exit()


if __name__ == "__main__":
    processar_tudo()
