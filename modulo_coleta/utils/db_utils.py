"""
utils/db_utils.py — Helpers para conexão e escrita no DuckDB.

O DuckDB é o banco analítico central do módulo de coleta. Cada município tem
seu próprio arquivo .duckdb em data/processed/{uf}_{municipio}/.

A extensão 'spatial' é carregada automaticamente na abertura da conexão,
permitindo armazenar e consultar geometrias diretamente no banco.

Funções públicas
----------------
abrir_conexao(db_path)
    Abre (ou cria) o arquivo .duckdb e ativa a extensão spatial.

salvar_geodataframe(conn, gdf, nome_tabela)
    Persiste um GeoDataFrame como tabela no DuckDB via GeoParquet temporário.

salvar_dataframe(conn, df, nome_tabela)
    Persiste um DataFrame pandas como tabela no DuckDB.

listar_tabelas(conn)
    Retorna lista com os nomes das tabelas existentes no banco.
"""

from __future__ import annotations

import logging
import tempfile
from pathlib import Path

import duckdb
import geopandas as gpd
import pandas as pd

logger = logging.getLogger(__name__)


def abrir_conexao(db_path: Path | str) -> duckdb.DuckDBPyConnection:
    """
    Abre (ou cria) um arquivo DuckDB e ativa a extensão spatial.

    A extensão spatial permite armazenar geometrias WKB e executar funções
    como ST_Area, ST_Intersects etc. diretamente no banco.

    Parâmetros
    ----------
    db_path : Path | str
        Caminho para o arquivo .duckdb.
        Se não existir, é criado automaticamente.

    Retorna
    -------
    duckdb.DuckDBPyConnection
        Conexão aberta. Chamar .close() ao terminar.

    Exemplo
    -------
    >>> conn = abrir_conexao("data/processed/sp_jacarei/jacarei.duckdb")
    >>> conn.execute("SELECT * FROM limite_municipal").fetchdf()
    >>> conn.close()
    """
    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    conn = duckdb.connect(str(db_path))

    # Instala e carrega a extensão spatial (baixa uma única vez, fica em cache)
    conn.execute("INSTALL spatial")
    conn.execute("LOAD spatial")

    logger.info("DuckDB aberto: %s", db_path)
    return conn


def salvar_geodataframe(
    conn: duckdb.DuckDBPyConnection,
    gdf: gpd.GeoDataFrame,
    nome_tabela: str,
    substituir: bool = True,
) -> None:
    """
    Persiste um GeoDataFrame como tabela no DuckDB.

    Estratégia: exporta para GeoParquet temporário → DuckDB lê com
    ST_GeomFromWKB, garantindo que a geometria fica em coluna 'geometry'
    no formato WKB (compatível com a extensão spatial).

    O CRS é validado antes de salvar: apenas EPSG:4674 é aceito.
    Se o GeoDataFrame estiver em outro CRS, é reprojetado automaticamente
    com aviso no log.

    Parâmetros
    ----------
    conn : duckdb.DuckDBPyConnection
        Conexão aberta via abrir_conexao().
    gdf : gpd.GeoDataFrame
        GeoDataFrame a persistir. Deve ter coluna 'geometry'.
    nome_tabela : str
        Nome da tabela no DuckDB. Ex: "setores_censitarios".
    substituir : bool
        Se True (padrão), recria a tabela se já existir (CREATE OR REPLACE).
        Se False, lança erro caso a tabela exista.

    Retorna
    -------
    None

    Levanta
    -------
    ValueError
        Se o GeoDataFrame estiver vazio ou sem coluna geometry.
    """
    if gdf is None or gdf.empty:
        raise ValueError(f"GeoDataFrame vazio — tabela '{nome_tabela}' não será salva.")
    if "geometry" not in gdf.columns:
        raise ValueError(f"GeoDataFrame sem coluna 'geometry' — tabela '{nome_tabela}'.")

    # Garantir CRS correto
    crs_atual = gdf.crs
    if crs_atual is None:
        logger.warning("GeoDataFrame sem CRS definido — assumindo EPSG:4674.")
        gdf = gdf.set_crs("EPSG:4674")
    elif crs_atual.to_epsg() != 4674:
        logger.warning(
            "CRS %s detectado em '%s' — reprojetando para EPSG:4674.",
            crs_atual.to_string(),
            nome_tabela,
        )
        gdf = gdf.to_crs("EPSG:4674")

    # Usar arquivo temporário para a ponte GeoDataFrame → DuckDB
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
        tmp_path = Path(tmp.name)

    try:
        # Converte geometria para WKB (bytes) antes de salvar no parquet.
        # Isso evita o erro de incompatibilidade de CRS do DuckDB spatial
        # com o formato GeoArrow nativo do geopandas.
        df_para_salvar = gdf.copy()
        df_para_salvar["geometry"] = gdf.geometry.apply(lambda geom: geom.wkb)

        df_para_salvar.to_parquet(tmp_path, index=False)

        modo = "CREATE OR REPLACE TABLE" if substituir else "CREATE TABLE"
        conn.execute(f"""
            {modo} {nome_tabela} AS
            SELECT
                * EXCLUDE (geometry),
                ST_GeomFromWKB(geometry) AS geometry
            FROM read_parquet('{tmp_path.as_posix()}')
        """)

        n = conn.execute(f"SELECT COUNT(*) FROM {nome_tabela}").fetchone()[0]
        logger.info("Tabela '%s' salva: %d registros.", nome_tabela, n)

    finally:
        tmp_path.unlink(missing_ok=True)


def salvar_dataframe(
    conn: duckdb.DuckDBPyConnection,
    df: pd.DataFrame,
    nome_tabela: str,
    substituir: bool = True,
) -> None:
    """
    Persiste um DataFrame pandas como tabela no DuckDB.

    O DuckDB consegue ler DataFrames pandas diretamente via referência à
    variável Python — não é necessário exportar para arquivo intermediário.

    Parâmetros
    ----------
    conn : duckdb.DuckDBPyConnection
        Conexão aberta via abrir_conexao().
    df : pd.DataFrame
        DataFrame a persistir.
    nome_tabela : str
        Nome da tabela no DuckDB. Ex: "censo_domicilio01".
    substituir : bool
        Se True (padrão), recria a tabela se já existir.

    Retorna
    -------
    None

    Levanta
    -------
    ValueError
        Se o DataFrame estiver vazio.
    """
    if df is None or df.empty:
        raise ValueError(f"DataFrame vazio — tabela '{nome_tabela}' não será salva.")

    modo = "CREATE OR REPLACE TABLE" if substituir else "CREATE TABLE"
    # DuckDB reconhece 'df' como variável Python automaticamente
    conn.execute(f"{modo} {nome_tabela} AS SELECT * FROM df")

    n = conn.execute(f"SELECT COUNT(*) FROM {nome_tabela}").fetchone()[0]
    logger.info("Tabela '%s' salva: %d registros.", nome_tabela, n)


def listar_tabelas(conn: duckdb.DuckDBPyConnection) -> list[str]:
    """
    Retorna os nomes de todas as tabelas no banco DuckDB.

    Parâmetros
    ----------
    conn : duckdb.DuckDBPyConnection
        Conexão aberta via abrir_conexao().

    Retorna
    -------
    list[str]
        Lista de nomes de tabelas. Ex: ["limite_municipal", "setores_censitarios"]
    """
    resultado = conn.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
    ).fetchall()
    return [row[0] for row in resultado]
