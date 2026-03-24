"""
grupo1_geometrias.py — Coleta de geometrias base do município.

Camadas coletadas:
    1. Limite municipal      → geobr.read_municipality()
    2. Setores censitários   → FTP IBGE .gpkg por UF, filtro por CD_MUN
    3. Grade estatística     → cruzamento com BR500KM → download por quadrante
    4. Áreas de ponderação   → geobr.read_weighting_area()

Fluxo da grade estatística:
    a) Baixa grade de referência 500km (BR500KM.zip, arquivo pequeno)
    b) Cruzamento espacial: polígono municipal × grade 500km → coluna QUADRANTE
    c) Para cada quadrante, baixa grade_id{N}.zip de grade_estatistica/
    d) Clip pelo polígono municipal; concatena se >1 quadrante

Tabelas DuckDB geradas:
    limite_municipal, setores_censitarios, grade_estatistica, areas_ponderacao

Dependências: utils/ibge_ftp.py, utils/db_utils.py
"""

from __future__ import annotations

import logging
import zipfile
from pathlib import Path

import geopandas as gpd
import pandas as pd

from ..utils.db_utils import salvar_geodataframe
from ..utils.ibge_ftp import baixar_arquivo, baixar_setores_censitarios, descompactar_zip

logger = logging.getLogger(__name__)

_URL_GRADE_500KM = (
    "https://geoftp.ibge.gov.br/recortes_para_fins_estatisticos"
    "/grade_estatistica/censo_2022/grade_500km/BR500KM.zip"
)
_URL_BASE_GRADE_ESTATISTICA = (
    "https://geoftp.ibge.gov.br/recortes_para_fins_estatisticos"
    "/grade_estatistica/censo_2022/grade_estatistica/"
)


# ---------------------------------------------------------------------------
# Funções auxiliares
# ---------------------------------------------------------------------------

def _baixar_limite_municipal(codigo_ibge: str) -> gpd.GeoDataFrame:
    """Baixa o polígono do limite municipal via geobr."""
    import geobr
    logger.info("Baixando limite municipal via geobr (codigo_ibge=%s)", codigo_ibge)
    gdf = geobr.read_municipality(code_muni=int(codigo_ibge), year=2022)
    gdf = gdf.to_crs("EPSG:4674")
    return gdf


def _baixar_areas_ponderacao(codigo_ibge: str) -> gpd.GeoDataFrame:
    """Baixa as áreas de ponderação do município via geobr."""
    import geobr
    logger.info("Baixando areas de ponderacao via geobr (codigo_ibge=%s)", codigo_ibge)
    gdf = geobr.read_weighting_area(code_weighting=int(codigo_ibge), year=2010)
    gdf = gdf.to_crs("EPSG:4674")
    return gdf


def _identificar_quadrantes(
    poligono_mun: gpd.GeoDataFrame,
    cache_dir: Path,
) -> list[str]:
    """
    Cruza o polígono municipal com a grade BR500KM para identificar
    em quais quadrantes o município está inserido.

    Retorna lista de strings no formato 'ID_50', 'ID_51', etc.
    """
    zip_500 = baixar_arquivo(
        _URL_GRADE_500KM,
        cache_dir / "BR500KM.zip",
    )

    # Extrai apenas se ainda não extraído
    gpkg_500 = cache_dir / "BR500KM.gpkg"
    shp_500_candidates = list(cache_dir.glob("BR500KM*.shp"))

    if not gpkg_500.exists() and not shp_500_candidates:
        extraidos = descompactar_zip(zip_500, cache_dir)
        shp_500_candidates = [p for p in extraidos if p.suffix.lower() == ".shp"]

    # Lê a grade 500km
    if gpkg_500.exists():
        grade_500 = gpd.read_file(gpkg_500)
    elif shp_500_candidates:
        grade_500 = gpd.read_file(shp_500_candidates[0])
    else:
        # Tenta ler direto do ZIP se o formato suportado
        grade_500 = gpd.read_file(f"zip://{zip_500}")

    grade_500 = grade_500.to_crs("EPSG:4674")

    # Cruzamento espacial
    poligono_reproj = poligono_mun.to_crs("EPSG:4674")
    cruzamento = gpd.sjoin(grade_500, poligono_reproj, how="inner", predicate="intersects")

    quadrantes = cruzamento["QUADRANTE"].unique().tolist()
    logger.info("Quadrantes identificados para o municipio: %s", quadrantes)
    return quadrantes


def _baixar_grade_quadrante(
    quadrante: str,
    output_dir: Path,
    forcar: bool = False,
) -> gpd.GeoDataFrame:
    """
    Baixa e lê a grade estatística de um quadrante específico.

    Parâmetros
    ----------
    quadrante : str
        Valor da coluna QUADRANTE na grade 500km. Ex: 'ID_50'.
        O nome do arquivo segue o padrão grade_id50.zip (minúsculo, sem '_').
    """
    # 'ID_50' → 'id50'
    nome_id = quadrante.lower().replace("_", "")
    nome_zip = f"grade_{nome_id}.zip"
    url = _URL_BASE_GRADE_ESTATISTICA + nome_zip
    zip_path = baixar_arquivo(url, output_dir / nome_zip, forcar=forcar)

    ext_dir = output_dir / nome_id
    if not ext_dir.exists() or forcar:
        descompactar_zip(zip_path, ext_dir)

    # Lê o primeiro shapefile encontrado dentro do ZIP extraído
    shps = list(ext_dir.rglob("*.shp"))
    gpkgs = list(ext_dir.rglob("*.gpkg"))

    if gpkgs:
        gdf = gpd.read_file(gpkgs[0])
    elif shps:
        gdf = gpd.read_file(shps[0])
    else:
        raise FileNotFoundError(
            f"Nenhum arquivo vetorial encontrado em {ext_dir} "
            f"(quadrante={quadrante})"
        )

    return gdf.to_crs("EPSG:4674")


# ---------------------------------------------------------------------------
# Função principal do grupo
# ---------------------------------------------------------------------------

def coletar_grupo1(
    codigo_ibge: str,
    limite_municipal: gpd.GeoDataFrame | None,
    output_dir: Path,
    db_conn,
    forcar: bool = False,
    **kwargs,
) -> dict:
    """
    Coleta geometrias base para o município e persiste no DuckDB.

    Parâmetros
    ----------
    codigo_ibge : str
        Código IBGE de 7 dígitos. Ex: "2701407".
    limite_municipal : gpd.GeoDataFrame | None
        Se já disponível, reutiliza. Se None, baixa via geobr.
    output_dir : Path
        Pasta de saída: data/raw/{uf}_{municipio}/geometria/
    db_conn : duckdb.DuckDBPyConnection
        Conexão aberta com o DuckDB do município.
    forcar : bool
        Se True, rebaixa arquivos mesmo que já existam.

    Retorna
    -------
    dict
        {"status": "ok"|"erro", "camadas": [...], "mensagem": "..."}
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    cache_grade = output_dir / "_cache_grade"
    cache_grade.mkdir(exist_ok=True)

    camadas_salvas = []

    try:
        # --- 1. Limite municipal ---
        logger.info("[Grupo 1] Limite municipal")
        if limite_municipal is None:
            limite_municipal = _baixar_limite_municipal(codigo_ibge)
        limite_municipal = limite_municipal.to_crs("EPSG:4674")
        limite_municipal.to_file(output_dir / "limite_municipal.gpkg", driver="GPKG")
        salvar_geodataframe(db_conn, limite_municipal, "limite_municipal")
        camadas_salvas.append("limite_municipal")
        logger.info("[Grupo 1] Limite municipal OK")

        # --- 2. Setores censitários ---
        logger.info("[Grupo 1] Setores censitarios")
        gpkg_uf = baixar_setores_censitarios(codigo_ibge, output_dir, forcar=forcar)
        setores_uf = gpd.read_file(gpkg_uf)
        setores_mun = setores_uf[setores_uf["CD_MUN"] == codigo_ibge].copy()
        setores_mun = setores_mun.to_crs("EPSG:4674")
        setores_mun.to_file(output_dir / "setores_censitarios.gpkg", driver="GPKG")
        salvar_geodataframe(db_conn, setores_mun, "setores_censitarios")
        camadas_salvas.append("setores_censitarios")
        logger.info("[Grupo 1] Setores censitarios OK: %d setores", len(setores_mun))

        # --- 3. Grade estatística ---
        logger.info("[Grupo 1] Grade estatistica")
        quadrantes = _identificar_quadrantes(limite_municipal, cache_grade)

        grades = []
        for q in quadrantes:
            logger.info("[Grupo 1] Baixando grade quadrante %s", q)
            gdf_q = _baixar_grade_quadrante(q, cache_grade, forcar=forcar)
            grades.append(gdf_q)

        grade = pd.concat(grades, ignore_index=True) if len(grades) > 1 else grades[0]
        grade = gpd.GeoDataFrame(grade, crs="EPSG:4674")

        # Clip pelo polígono municipal
        poligono = limite_municipal.geometry.union_all()
        grade_mun = grade[grade.geometry.intersects(poligono)].copy()
        grade_mun.to_file(output_dir / "grade_estatistica.gpkg", driver="GPKG")
        salvar_geodataframe(db_conn, grade_mun, "grade_estatistica")
        camadas_salvas.append("grade_estatistica")
        logger.info("[Grupo 1] Grade estatistica OK: %d celulas", len(grade_mun))

        # --- 4. Áreas de ponderação ---
        logger.info("[Grupo 1] Areas de ponderacao")
        areas_pond = _baixar_areas_ponderacao(codigo_ibge)
        areas_pond.to_file(output_dir / "areas_ponderacao.gpkg", driver="GPKG")
        salvar_geodataframe(db_conn, areas_pond, "areas_ponderacao")
        camadas_salvas.append("areas_ponderacao")
        logger.info("[Grupo 1] Areas de ponderacao OK: %d areas", len(areas_pond))

        return {
            "status": "ok",
            "camadas": camadas_salvas,
            "mensagem": f"{len(camadas_salvas)} camadas coletadas com sucesso.",
        }

    except Exception as exc:
        logger.error("[Grupo 1] Erro: %s", exc, exc_info=True)
        return {
            "status": "erro",
            "camadas": camadas_salvas,
            "mensagem": str(exc),
        }
