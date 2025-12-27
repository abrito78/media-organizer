#!/usr/bin/env python3

import os
import shutil
import hashlib
import argparse
import logging
from datetime import datetime
import time
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed

def configurar_log(pasta_log):
    os.makedirs(pasta_log, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join(pasta_log, f"execucao_{timestamp}.log")

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[
            logging.FileHandler(log_file, encoding="utf-8"),
            logging.StreamHandler()
        ]
    )
    return log_file

def hash_arquivo(caminho, bloco=65536):
    h = hashlib.sha256()
    with open(caminho, 'rb') as f:
        for chunk in iter(lambda: f.read(bloco), b''):
            h.update(chunk)
    return h.hexdigest()

def calcular_hash_worker(caminho):
    try:
        return caminho, hash_arquivo(caminho)
    except Exception:
        return caminho, None

def agrupar_por_tamanho(pasta_origem, pasta_duplicados, estatisticas):
    grupos = defaultdict(list)

    for raiz, _, arquivos in os.walk(pasta_origem):
        for nome in arquivos:
            caminho = os.path.join(raiz, nome)

            if os.path.commonpath([caminho, pasta_duplicados]) == pasta_duplicados:
                continue

            estatisticas["arquivos_analisados"] += 1

            try:
                tamanho = os.path.getsize(caminho)
                grupos[tamanho].append(caminho)
            except OSError as e:
                estatisticas["erros"] += 1
                logging.error(f"Erro ao obter tamanho: {caminho} | {e}")

    return {t: arquivos for t, arquivos in grupos.items() if len(arquivos) > 1}

def identificar_e_mover_duplicados(pasta_origem, pasta_duplicados, arquivo_inventario, workers):
    hashes = {}
    estatisticas = {
        "arquivos_analisados": 0,
        "duplicados_encontrados": 0,
        "erros": 0
    }

    os.makedirs(pasta_duplicados, exist_ok=True)

    candidatos = agrupar_por_tamanho(pasta_origem, pasta_duplicados, estatisticas)

    with open(arquivo_inventario, "w", encoding="utf-8") as inventario:
        inventario.write("hash_sha256;arquivo\n")

        with ProcessPoolExecutor(max_workers=workers) as executor:
            for _, arquivos in candidatos.items():
                futures = [executor.submit(calcular_hash_worker, caminho) for caminho in arquivos]

                for future in as_completed(futures):
                    caminho, h = future.result()

                    if h is None:
                        estatisticas["erros"] += 1
                        logging.error(f"Erro ao gerar hash: {caminho}")
                        continue

                    inventario.write(f"{h};{caminho}\n")

                    if h in hashes:
                        estatisticas["duplicados_encontrados"] += 1
                        nome = os.path.basename(caminho)
                        destino = os.path.join(pasta_duplicados, nome)
                        contador = 1

                        while os.path.exists(destino):
                            base, ext = os.path.splitext(nome)
                            destino = os.path.join(
                                pasta_duplicados, f"{base}_{contador}{ext}"
                            )
                            contador += 1

                        shutil.move(caminho, destino)
                        logging.info(f"DUPLICADO | {caminho} -> {destino}")
                    else:
                        hashes[h] = caminho

    return estatisticas

def main():
    inicio = time.time()

    parser = argparse.ArgumentParser(
        description="Identifica arquivos duplicados por hash com pré-filtro por tamanho e multiprocessamento."
    )
    parser.add_argument("origem", help="Pasta raiz para varredura")
    parser.add_argument(
        "--duplicados",
        default="duplicados",
        help="Pasta destino dos arquivos duplicados"
    )
    parser.add_argument(
        "--logs",
        default="logs",
        help="Pasta para armazenar logs"
    )
    parser.add_argument(
        "--inventario",
        default="inventorio_hash.txt",
        help="Arquivo de inventário hash"
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=os.cpu_count(),
        help="Número de processos para hashing (default: CPUs disponíveis)"
    )

    args = parser.parse_args()

    pasta_origem = os.path.abspath(args.origem)
    pasta_duplicados = os.path.abspath(args.duplicados)
    pasta_logs = os.path.abspath(args.logs)
    arquivo_inventario = os.path.abspath(args.inventario)

    log_file = configurar_log(pasta_logs)

    logging.info("Início da execução")
    logging.info(f"Pasta origem: {pasta_origem}")
    logging.info(f"Pasta duplicados: {pasta_duplicados}")
    logging.info(f"Inventário: {arquivo_inventario}")
    logging.info(f"Workers: {args.workers}")

    estatisticas = identificar_e_mover_duplicados(
        pasta_origem,
        pasta_duplicados,
        arquivo_inventario,
        args.workers
    )

    tempo_execucao = time.time() - inicio

    logging.info("Fim da execução")
    logging.info("Estatísticas finais:")
    logging.info(f"Arquivos analisados: {estatisticas['arquivos_analisados']}")
    logging.info(f"Duplicados encontrados: {estatisticas['duplicados_encontrados']}")
    logging.info(f"Erros: {estatisticas['erros']}")
    logging.info(f"Tempo total: {tempo_execucao:.2f}s")
    logging.info(f"Log gerado em: {log_file}")

    print("\nResumo da execução:")
    print(f"- Arquivos analisados: {estatisticas['arquivos_analisados']}")
    print(f"- Duplicados encontrados: {estatisticas['duplicados_encontrados']}")
    print(f"- Erros: {estatisticas['erros']}")
    print(f"- Tempo de execução: {tempo_execucao:.2f}s")
    print(f"- Inventário: {arquivo_inventario}")
    print(f"- Log: {log_file}")

if __name__ == "__main__":
    main()

