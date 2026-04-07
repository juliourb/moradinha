"""
grupo6_extensoes.py — Stubs para grupos de dados futuros.

⚠️ NÃO IMPLEMENTAR — apenas docstrings de referência.

Grupos planejados:
    6a: Aglomerados subnormais / favelas (geobr::read_urban_concentrations)
    6b: CNES — estabelecimentos de saúde (API DATASUS)
    6c: INEP — escolas geocodificadas (dados.gov.br)
    6d: MapBiomas — cobertura e uso do solo (GEE ou download por bbox)
    6e: CadÚnico — famílias geocodificadas (requer convênio institucional)
"""

from __future__ import annotations
from pathlib import Path
import geopandas as gpd
import duckdb


def coletar_grupo6a_aglomerados(
    codigo_ibge: str,
    limite_municipal: gpd.GeoDataFrame,
    output_dir: Path,
    db_conn: duckdb.DuckDBPyConnection,
    **kwargs,
) -> dict:
    """
    [STUB] Aglomerados subnormais e favelas via geobr.

    Fonte: IBGE — geobr::read_urban_concentrations()
    Tabela DuckDB: aglomerados_subnormais
    """
    raise NotImplementedError("grupo6a_aglomerados não implementado (stub).")


def coletar_grupo6b_cnes(
    codigo_ibge: str,
    limite_municipal: gpd.GeoDataFrame,
    output_dir: Path,
    db_conn: duckdb.DuckDBPyConnection,
    **kwargs,
) -> dict:
    """
    [STUB] Estabelecimentos de saúde CNES via API DATASUS.

    Fonte: datasus.saude.gov.br
    Tabela DuckDB: estabelecimentos_saude
    """
    raise NotImplementedError("grupo6b_cnes não implementado (stub).")


def coletar_grupo6c_inep(
    codigo_ibge: str,
    limite_municipal: gpd.GeoDataFrame,
    output_dir: Path,
    db_conn: duckdb.DuckDBPyConnection,
    **kwargs,
) -> dict:
    """
    [STUB] Escolas geocodificadas via INEP / dados.gov.br.

    Fonte: dados.gov.br — Censo Escolar
    Tabela DuckDB: escolas
    """
    raise NotImplementedError("grupo6c_inep não implementado (stub).")


def coletar_grupo6d_mapbiomas(
    codigo_ibge: str,
    limite_municipal: gpd.GeoDataFrame,
    output_dir: Path,
    db_conn: duckdb.DuckDBPyConnection,
    **kwargs,
) -> dict:
    """
    [STUB] Cobertura e uso do solo MapBiomas.

    Fonte: mapbiomas.org (GEE ou download por bbox)
    Tabela DuckDB: cobertura_solo
    """
    raise NotImplementedError("grupo6d_mapbiomas não implementado (stub).")


def coletar_grupo6e_cadunico(
    codigo_ibge: str,
    limite_municipal: gpd.GeoDataFrame,
    output_dir: Path,
    db_conn: duckdb.DuckDBPyConnection,
    **kwargs,
) -> dict:
    """
    [STUB] Famílias CadÚnico geocodificadas.

    Fonte: MDS — requer convênio institucional.
    Tabela DuckDB: cadunico
    """
    raise NotImplementedError("grupo6e_cadunico não implementado (stub).")
