"""
orquestrador.py — Ponto de entrada do módulo de coleta.

Função principal:
    coletar_municipio(codigo_ibge, grupos, base_dir)

Uso:
    from moradinha.modulo_coleta.orquestrador import coletar_municipio
    coletar_municipio("2704302", grupos=[1, 2, 3])  # Maceió - AL
"""

from __future__ import annotations

import logging
from pathlib import Path

import duckdb
import geopandas as gpd

logger = logging.getLogger(__name__)

# Mapa de grupos disponíveis — adicionar novos grupos aqui sem alterar código abaixo
GRUPOS_DISPONIVEIS: dict[int, str] = {
    1: "grupo1_geometrias",
    2: "grupo2_censo",
    3: "grupo3_logradouros",
    4: "grupo4_luminosidade",
    5: "grupo5_pnadc",
    6: "grupo6_extensoes",
}


def coletar_municipio(
    codigo_ibge: str,
    grupos: list[int] = [1, 2, 3, 4, 5],
    base_dir: Path = Path("data"),
) -> None:
    """
    Coleta todos os dados necessários para um município e persiste em DuckDB.

    Parâmetros
    ----------
    codigo_ibge : str
        Código IBGE do município com 7 dígitos. Ex: "3524402" (Jacareí-SP).
    grupos : list[int]
        Quais grupos de dados coletar. Padrão: [1, 2, 3, 4, 5].
        1=Geometrias, 2=Censo, 3=Logradouros, 4=Luminosidade, 5=PNADc.
    base_dir : Path
        Pasta raiz dos dados. Padrão: "data/" relativa ao diretório de trabalho.

    Retorna
    -------
    None — os dados são persistidos em disco e no DuckDB.
    """
    # ⬜ TODO: implementar após validação individual de cada grupo
    raise NotImplementedError(
        "orquestrador.py ainda não implementado. "
        "Implemente e valide cada grupo individualmente primeiro."
    )
