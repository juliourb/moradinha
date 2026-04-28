"""
grupo3_logradouros.py — Coleta de endereços e logradouros.

Camadas coletadas:
    1. Endereços CNEFE   → FTP IBGE (ZIP por UF), filtro espacial pelo polígono
    2. Faces de logradouro IBGE → FTP IBGE (ZIP por UF), filtro por CD_MUN no shapefile
    3. Eixos viários OSM → OSMnx graph_from_polygon → edges

Refatorado de: template_dados_IBGE_por_municipio.ipynb
    obter_endereços()                   → _carregar_cnefe()
    baixar_faces_logradouros_municipio() → _carregar_faces_logradouro()
    baixar_eixos_osm()                  → utils/osmx.py

Tabelas DuckDB geradas:
    enderecos_cnefe             — bruta (todos os COD_ESPECIE, todas as qualidades)
    enderecos_cnefe_residencial — COD_ESPECIE=1 (domicílios particulares)
    enderecos_cnefe_naoresidencial — COD_ESPECIE ∈ {4,5,6,8} (ensino, saúde, outras, religiosos)
    faces_logradouro, eixos_osm

Todas as tabelas CNEFE incluem coluna qualidade_geo (alta/media/baixa) derivada de
NV_GEO_COORD, para filtragem downstream quando a covariável depende de localização fina.

COD_ESPECIE=2 (domicílio coletivo) e COD_ESPECIE=7 (em construção) ficam apenas na
tabela bruta — não integram as tabelas filtradas.

Dependências: utils/ibge_ftp.py, utils/osmx.py, utils/db_utils.py
"""

from __future__ import annotations

import logging
import tempfile
from pathlib import Path

import geopandas as gpd
import pandas as pd

from ..utils.db_utils import salvar_geodataframe
from ..utils.ibge_ftp import (
    baixar_cnefe,
    baixar_faces_logradouros,
    descompactar_zip,
    obter_sigla_uf,
)
from ..utils.osmx import baixar_eixos_osm

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Constantes CNEFE
# ---------------------------------------------------------------------------

_NV_GEO_QUALIDADE: dict[str, str] = {
    "1": "alta", "2": "alta",
    "3": "media", "4": "media",
    "5": "baixa", "6": "baixa",
}

_COD_ESPECIE_RESIDENCIAL:    frozenset[str] = frozenset({"1"})
_COD_ESPECIE_NAORESIDENCIAL: frozenset[str] = frozenset({"4", "5", "6", "8"})


def _col_by_upper(df: pd.DataFrame, nome_upper: str) -> str | None:
    """Returns the first column whose upper-case name matches nome_upper, or None."""
    return next((c for c in df.columns if c.upper() == nome_upper), None)


# ---------------------------------------------------------------------------
# Funções auxiliares
# ---------------------------------------------------------------------------

def _carregar_cnefe(
    codigo_ibge: str,
    poligono_mun: gpd.GeoDataFrame,
    output_dir: Path,
    forcar: bool = False,
) -> gpd.GeoDataFrame:
    """
    Baixa o ZIP CNEFE da UF, lê o CSV de endereços, filtra pelo polígono
    municipal e retorna GeoDataFrame de pontos em EPSG:4674.

    Refatorado de: obter_endereços() do template notebook.

    O ZIP contém um CSV com colunas LATITUDE e LONGITUDE para todos os
    endereços da UF. O filtro espacial usa .within(poligono), que pode ser
    lento para estados grandes. Os dados são mantidos em memória durante
    o processamento — o ZIP não é extraído para disco.
    """
    import zipfile, io

    zip_path = baixar_cnefe(codigo_ibge, output_dir, forcar=forcar)

    logger.info("Lendo CSV CNEFE de %s...", zip_path.name)
    with zipfile.ZipFile(zip_path) as z:
        csvs = [n for n in z.namelist() if n.lower().endswith(".csv")]
        if not csvs:
            raise FileNotFoundError(f"Nenhum CSV encontrado em {zip_path.name}")
        with z.open(csvs[0]) as f:
            df = pd.read_csv(
                io.TextIOWrapper(f, encoding="latin-1"),
                sep=";",
                dtype=str,
                low_memory=False,
            )

    # Normaliza nomes das colunas de coordenada (podem variar entre UFs)
    col_map = {}
    for col in df.columns:
        if col.upper() in ("LATITUDE", "LAT"):
            col_map[col] = "LATITUDE"
        elif col.upper() in ("LONGITUDE", "LON", "LONG"):
            col_map[col] = "LONGITUDE"
    if col_map:
        df = df.rename(columns=col_map)

    if "LATITUDE" not in df.columns or "LONGITUDE" not in df.columns:
        raise KeyError(
            f"Colunas de coordenada não encontradas. "
            f"Disponíveis: {list(df.columns[:10])}"
        )

    df["LATITUDE"]  = pd.to_numeric(df["LATITUDE"],  errors="coerce")
    df["LONGITUDE"] = pd.to_numeric(df["LONGITUDE"], errors="coerce")
    df = df.dropna(subset=["LATITUDE", "LONGITUDE"])

    gdf = gpd.GeoDataFrame(
        df,
        geometry=gpd.points_from_xy(df["LONGITUDE"], df["LATITUDE"]),
        crs="EPSG:4674",
    )

    # Filtro espacial: mantém apenas endereços dentro do polígono municipal
    poligono = poligono_mun.to_crs("EPSG:4674").geometry.union_all()
    gdf_mun = gdf[gdf.geometry.within(poligono)].copy()
    logger.info(
        "CNEFE: %d/%d endereços retidos para o município",
        len(gdf_mun), len(gdf),
    )

    # Coluna qualidade_geo derivada de NV_GEO_COORD
    col_nv = _col_by_upper(gdf_mun, "NV_GEO_COORD")
    if col_nv:
        gdf_mun["qualidade_geo"] = (
            gdf_mun[col_nv].astype(str).str.strip()
            .map(_NV_GEO_QUALIDADE)
            .fillna("desconhecida")
        )
    else:
        logger.warning("CNEFE: coluna NV_GEO_COORD não encontrada — qualidade_geo='desconhecida'.")
        gdf_mun["qualidade_geo"] = "desconhecida"

    return gdf_mun


def _carregar_faces_logradouro(
    codigo_ibge: str,
    output_dir: Path,
    forcar: bool = False,
) -> gpd.GeoDataFrame:
    """
    Baixa o ZIP de faces de logradouro da UF, extrai o shapefile do município
    (identificado pelo prefixo do código IBGE no nome do arquivo) e retorna
    GeoDataFrame em EPSG:4674.

    Refatorado de: baixar_faces_logradouros_municipio() do template notebook.

    Dentro do ZIP há um shapefile por município, nomeado com os 7 dígitos
    do código IBGE (ex: 2701407_faces_logradouros_2021.shp).
    """
    zip_path = baixar_faces_logradouros(codigo_ibge, output_dir, forcar=forcar)

    ext_dir = output_dir / f"_faces_{obter_sigla_uf(codigo_ibge).lower()}"
    if not ext_dir.exists() or forcar:
        descompactar_zip(zip_path, ext_dir)

    # Localiza o shapefile do município pelo prefixo do código IBGE
    shps = [
        p for p in ext_dir.rglob("*.shp")
        if p.stem.startswith(codigo_ibge)
    ]
    if not shps:
        raise FileNotFoundError(
            f"Shapefile para município {codigo_ibge} não encontrado em {ext_dir}. "
            f"Shapefiles disponíveis: {[p.name for p in ext_dir.rglob('*.shp')[:5]]}"
        )

    gdf = gpd.read_file(shps[0]).to_crs("EPSG:4674")
    logger.info("Faces de logradouro: %d faces carregadas", len(gdf))
    return gdf


# ---------------------------------------------------------------------------
# Função principal do grupo
# ---------------------------------------------------------------------------

def coletar_grupo3(
    codigo_ibge: str,
    limite_municipal: gpd.GeoDataFrame,
    output_dir: Path,
    db_conn,
    forcar: bool = False,
    network_type: str = "all",
    **kwargs,
) -> dict:
    """
    Coleta endereços e logradouros para o município e persiste no DuckDB.

    Parâmetros
    ----------
    codigo_ibge : str
        Código IBGE de 7 dígitos. Ex: "2701407".
    limite_municipal : gpd.GeoDataFrame
        Polígono do município — obrigatório para filtro CNEFE e OSM.
        Se None, levanta ValueError.
    output_dir : Path
        Pasta de saída: data/raw/{uf}_{municipio}/logradouros/
    db_conn : duckdb.DuckDBPyConnection
        Conexão aberta com o DuckDB do município.
    forcar : bool
        Se True, rebaixa arquivos mesmo que já existam.
    network_type : str
        Tipo de rede OSM para eixos viários. Padrão: 'all'.

    Retorna
    -------
    dict
        {"status": "ok"|"erro", "camadas": [...], "mensagem": "..."}
    """
    if limite_municipal is None:
        raise ValueError("limite_municipal é obrigatório para o Grupo 3.")

    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    camadas_salvas = []

    try:
        # --- 1. Endereços CNEFE ---
        logger.info("[Grupo 3] Endereços CNEFE")
        gdf_cnefe = _carregar_cnefe(codigo_ibge, limite_municipal, output_dir, forcar)

        # Tabela bruta (todas as espécies, todas as qualidades)
        gdf_cnefe.to_file(output_dir / "enderecos_cnefe.gpkg", driver="GPKG")
        salvar_geodataframe(db_conn, gdf_cnefe, "enderecos_cnefe")
        camadas_salvas.append("enderecos_cnefe")

        # Tabelas filtradas por COD_ESPECIE
        col_esp = _col_by_upper(gdf_cnefe, "COD_ESPECIE")
        if col_esp:
            cod = gdf_cnefe[col_esp].astype(str).str.strip()

            gdf_resid = gdf_cnefe[cod.isin(_COD_ESPECIE_RESIDENCIAL)].copy()
            salvar_geodataframe(db_conn, gdf_resid, "enderecos_cnefe_residencial")
            camadas_salvas.append("enderecos_cnefe_residencial")

            gdf_naoresid = gdf_cnefe[cod.isin(_COD_ESPECIE_NAORESIDENCIAL)].copy()
            salvar_geodataframe(db_conn, gdf_naoresid, "enderecos_cnefe_naoresidencial")
            camadas_salvas.append("enderecos_cnefe_naoresidencial")

            dist_qual = gdf_cnefe["qualidade_geo"].value_counts().to_dict()
            logger.info(
                "[Grupo 3] CNEFE OK: %d total | %d residencial | %d não-residencial | qualidade: %s",
                len(gdf_cnefe), len(gdf_resid), len(gdf_naoresid), dist_qual,
            )
        else:
            logger.warning("[Grupo 3] COD_ESPECIE não encontrada — apenas tabela bruta gerada.")
            logger.info("[Grupo 3] CNEFE OK: %d endereços", len(gdf_cnefe))

        # --- 2. Faces de logradouro ---
        logger.info("[Grupo 3] Faces de logradouro")
        gdf_faces = _carregar_faces_logradouro(codigo_ibge, output_dir, forcar)
        gdf_faces.to_file(output_dir / "faces_logradouro.gpkg", driver="GPKG")
        salvar_geodataframe(db_conn, gdf_faces, "faces_logradouro")
        camadas_salvas.append("faces_logradouro")
        logger.info("[Grupo 3] Faces OK: %d faces", len(gdf_faces))

        # --- 3. Eixos OSM ---
        logger.info("[Grupo 3] Eixos OSM")
        gpkg_eixos = output_dir / "eixos_osm.gpkg"
        if gpkg_eixos.exists() and not forcar:
            logger.info("Arquivo já existe (pulando download): %s", gpkg_eixos)
            gdf_eixos = gpd.read_file(gpkg_eixos)
        else:
            gdf_eixos = baixar_eixos_osm(limite_municipal, network_type=network_type)
            gdf_eixos.to_file(gpkg_eixos, driver="GPKG")
        salvar_geodataframe(db_conn, gdf_eixos, "eixos_osm")
        camadas_salvas.append("eixos_osm")
        logger.info("[Grupo 3] Eixos OSM OK: %d arestas", len(gdf_eixos))

        return {
            "status": "ok",
            "camadas": camadas_salvas,
            "mensagem": f"{len(camadas_salvas)} camadas coletadas.",
        }

    except Exception as exc:
        logger.error("[Grupo 3] Erro: %s", exc, exc_info=True)
        return {
            "status": "erro",
            "camadas": camadas_salvas,
            "mensagem": str(exc),
        }
