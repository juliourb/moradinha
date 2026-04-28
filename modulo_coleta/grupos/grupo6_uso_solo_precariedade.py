"""
grupo6_uso_solo_precariedade.py — Uso e cobertura do solo (MapBiomas) + precariedade (FCU).

Fontes:
    MapBiomas Coleção 10 — rasters anuais de uso e cobertura do solo (tile local)
    FCU (Favelas e Comunidades Urbanas) — shapefile IBGE Censo 2022

Fluxo MapBiomas:
    tile_local (brazil_coverage_{ano}.tif em data/raw/tiles_globais/)
      → clip pelo limite municipal
      → zonal stats categórico por setor (rasterstats categorical=True)
      → proporções por 6 classes agregadas (classe 27 excluída do denominador)
      → DuckDB: mapbiomas_{ano}

Fluxo FCU:
    FTP IBGE → ZIP nacional → shapefile
      → filtro por CD_MUN (ou intersecção espacial como fallback)
      → intersecção com setores_censitarios
      → DuckDB: fcu_municipio, fcu_setor

Tabelas DuckDB geradas:
    mapbiomas_{ano}   — prop_* por classe agregada + geometry, por setor (uma tabela por ano)
    fcu_municipio     — polígonos FCU recortados pelo município (vazia se nenhuma FCU)
    fcu_setor         — fcu_intersecta / fcu_area_pct / fcu_n_poligonos por setor

Pendente (sessão futura):
    mapbiomas_{ano}_dominio_pnadc — requer obter_geometria_dominio_pnadc() do Grupo 5

Dependências: utils/raster_utils.py, utils/db_utils.py, rasterstats, rasterio, requests
"""

from __future__ import annotations

import logging
import zipfile
from pathlib import Path

import geopandas as gpd
import pandas as pd
import requests

from ..utils.db_utils import salvar_geodataframe
from ..utils.raster_utils import clip_raster, ler_tabela_espacial

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Classes MapBiomas Coleção 10 — 6 categorias temáticas agregadas
# (confirmar ao migrar para Coleção 11+)
# ---------------------------------------------------------------------------

CLASSES_MAPBIOMAS_AGREGADAS: dict[str, frozenset[int]] = {
    "urbano":         frozenset({24}),
    "outras_nao_veg": frozenset({23, 25, 30, 75}),
    "agropecuaria":   frozenset({9, 15, 18, 19, 20, 35, 36, 39, 40, 41, 46, 47, 48, 62}),
    "mosaico_uso":    frozenset({21}),
    "vegetacao":      frozenset({1, 3, 4, 5, 6, 10, 11, 12, 29, 32, 49, 50}),
    "agua":           frozenset({26, 31, 33}),
    "nao_observado":  frozenset({27}),  # excluído do denominador
}

_URL_FCU = (
    "https://ftp.ibge.gov.br/Censos/Censo_Demografico_2022/"
    "Favelas_e_comunidades_urbanas_Resultados_do_universo/"
    "arquivos_vetoriais/poligonos_FCUs_shp.zip"
)


# ---------------------------------------------------------------------------
# Helpers MapBiomas
# ---------------------------------------------------------------------------

def _buscar_tile_mapbiomas(tile_dir: Path, ano: int) -> Path:
    """Locates the MapBiomas coverage tile for the given year in tile_dir."""
    exact = tile_dir / f"brazil_coverage_{ano}.tif"
    if exact.exists():
        return exact
    matches = sorted(tile_dir.glob(f"*coverage*{ano}*.tif"))
    if not matches:
        raise FileNotFoundError(
            f"Tile MapBiomas para {ano} não encontrado em {tile_dir}.\n"
            f"Esperado: brazil_coverage_{ano}.tif\n"
            f"Arquivos .tif disponíveis: {[p.name for p in tile_dir.glob('*.tif')]}"
        )
    return matches[-1]


def _props_por_setor(raw_stats: list[dict | None]) -> pd.DataFrame:
    """
    Converts categorical zonal stats (pixel counts per class code) to proportions.

    Class 27 (nao_observado) is excluded from the denominator. The residual
    is reported as area_observada_pct — sectors with full coverage have 1.0.

    Parameters
    ----------
    raw_stats : list of dicts
        Output of rasterstats.zonal_stats(..., categorical=True).
        Keys are integer class codes; values are pixel counts.

    Returns
    -------
    pd.DataFrame
        Columns: prop_urbano, prop_outras_nao_veg, prop_agropecuaria,
                 prop_mosaico_uso, prop_vegetacao, prop_agua, area_observada_pct.
        One row per feature (same order as raw_stats).
    """
    cat_names = [c for c in CLASSES_MAPBIOMAS_AGREGADAS if c != "nao_observado"]
    rows = []

    for stat in raw_stats:
        if not stat:
            row = {f"prop_{c}": float("nan") for c in cat_names}
            row["area_observada_pct"] = float("nan")
            rows.append(row)
            continue

        n_nao_obs = sum(
            v for k, v in stat.items()
            if int(k) in CLASSES_MAPBIOMAS_AGREGADAS["nao_observado"]
        )
        total = sum(stat.values())
        total_obs = total - n_nao_obs

        row = {}
        for cat_nome in cat_names:
            cat_classes = CLASSES_MAPBIOMAS_AGREGADAS[cat_nome]
            count_cat = sum(stat.get(k, 0) for k in cat_classes)
            row[f"prop_{cat_nome}"] = count_cat / total_obs if total_obs > 0 else float("nan")
        row["area_observada_pct"] = (total_obs / total) if total > 0 else float("nan")
        rows.append(row)

    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Helpers FCU
# ---------------------------------------------------------------------------

def _download_fcu(cache_dir: Path) -> Path:
    """Downloads the national FCU shapefile ZIP from IBGE FTP if not cached."""
    import time

    cache_dir.mkdir(parents=True, exist_ok=True)
    zip_path = cache_dir / "poligonos_FCUs_shp.zip"

    if zip_path.exists():
        logger.info("[Grupo 6] FCU: cache hit — %s", zip_path.name)
        return zip_path

    logger.info("[Grupo 6] FCU: baixando shapefile nacional de favelas/comunidades urbanas...")
    t0 = time.time()
    resp = requests.get(_URL_FCU, stream=True, timeout=600)
    resp.raise_for_status()

    total = int(resp.headers.get("content-length", 0))
    baixado = 0
    with open(zip_path, "wb") as f:
        for chunk in resp.iter_content(chunk_size=4 * 1024 * 1024):
            if chunk:
                f.write(chunk)
                baixado += len(chunk)
                if total:
                    logger.info(
                        "[Grupo 6] FCU: %.0f%% — %.1f / %.1f MB",
                        100 * baixado / total, baixado / 1e6, total / 1e6,
                    )
    logger.info(
        "[Grupo 6] FCU: download concluído — %.1f MB em %.0fs",
        zip_path.stat().st_size / 1e6, time.time() - t0,
    )
    return zip_path


def _ler_fcu_municipio(
    zip_path: Path,
    codigo_ibge: str,
    limite_municipal: gpd.GeoDataFrame,
    ext_dir: Path,
) -> gpd.GeoDataFrame:
    """
    Reads FCU polygons for the given municipality from the national ZIP.

    Uses CD_MUN attribute filter when available; falls back to spatial
    intersection with limite_municipal. Returns an empty GeoDataFrame
    (with geometry column) if no FCU found for the municipality.

    The national ZIP is extracted once to ext_dir to avoid repeated extraction
    and Windows file-locking issues with TemporaryDirectory.
    """
    if not ext_dir.exists():
        ext_dir.mkdir(parents=True, exist_ok=True)
        logger.info("[Grupo 6] FCU: extraindo shapefile nacional...")
        with zipfile.ZipFile(zip_path) as zf:
            zf.extractall(ext_dir)

    shps = list(ext_dir.rglob("*.shp"))
    if not shps:
        raise FileNotFoundError(f"Nenhum .shp encontrado em {ext_dir}")

    gdf_all = gpd.read_file(shps[0]).to_crs("EPSG:4674")

    col_mun = next(
        (c for c in gdf_all.columns if c.upper() in ("CD_MUN", "COD_MUN", "CODMUN")),
        None,
    )
    if col_mun:
        gdf_mun = gdf_all[gdf_all[col_mun].astype(str).str.strip() == str(codigo_ibge)].copy()
        logger.info(
            "[Grupo 6] FCU: %d polígonos via CD_MUN=%s",
            len(gdf_mun), codigo_ibge,
        )
    else:
        poligono = limite_municipal.to_crs("EPSG:4674").geometry.union_all()
        gdf_mun = gdf_all[gdf_all.intersects(poligono)].copy()
        logger.info(
            "[Grupo 6] FCU: %d polígonos via intersecção espacial (CD_MUN ausente)",
            len(gdf_mun),
        )

    return gdf_mun


def _calcular_fcu_por_setor(
    gdf_fcu: gpd.GeoDataFrame,
    gdf_setores: gpd.GeoDataFrame,
) -> pd.DataFrame:
    """
    Calculates FCU coverage indicators per census sector.

    Uses EPSG:5880 (Brazil Polyconic) for area calculations to avoid
    geographic CRS distortion.

    Returns
    -------
    pd.DataFrame
        Columns: fcu_intersecta (bool), fcu_area_pct (float 0-1),
                 fcu_n_poligonos (int). Indexed like gdf_setores.
        All zeros/False when gdf_fcu is empty.
    """
    from shapely.ops import unary_union

    if len(gdf_fcu) == 0:
        logger.info("[Grupo 6] FCU: nenhum polígono — atribuindo zeros a todos os setores.")
        return pd.DataFrame({
            "fcu_intersecta":  [False] * len(gdf_setores),
            "fcu_area_pct":    [0.0]   * len(gdf_setores),
            "fcu_n_poligonos": [0]     * len(gdf_setores),
        }, index=gdf_setores.index)

    gdf_fcu_proj     = gdf_fcu.to_crs(epsg=5880)
    gdf_setores_proj = gdf_setores.to_crs(epsg=5880)

    fcu_union = unary_union(gdf_fcu_proj.geometry)

    # Count FCU polygons per sector via spatial join
    sjoined = gpd.sjoin(
        gdf_setores_proj[["geometry"]].reset_index(),
        gdf_fcu_proj[["geometry"]].reset_index(drop=True),
        how="left",
        predicate="intersects",
    )
    n_per_setor = (
        sjoined.groupby("index")["index_right"]
        .count()
        .reindex(gdf_setores.index, fill_value=0)
    )

    # Area proportion via intersection with FCU union polygon
    fcu_area_pcts = []
    for setor_geom in gdf_setores_proj.geometry:
        if setor_geom is None or setor_geom.is_empty:
            fcu_area_pcts.append(0.0)
            continue
        inter = setor_geom.intersection(fcu_union)
        pct = inter.area / setor_geom.area if setor_geom.area > 0 else 0.0
        fcu_area_pcts.append(float(pct))

    return pd.DataFrame({
        "fcu_intersecta":  [n > 0 for n in n_per_setor],
        "fcu_area_pct":    fcu_area_pcts,
        "fcu_n_poligonos": n_per_setor.values,
    }, index=gdf_setores.index)


# ---------------------------------------------------------------------------
# Sub-blocos
# ---------------------------------------------------------------------------

def _coletar_mapbiomas(
    limite_municipal: gpd.GeoDataFrame,
    output_dir: Path,
    tile_dir: Path,
    anos: list[int],
    db_conn,
    forcar: bool,
) -> list[str]:
    """Clips MapBiomas tiles and computes per-sector class proportions."""
    import rasterio
    from rasterstats import zonal_stats

    camadas_salvas = []

    for ano in anos:
        nome_tabela  = f"mapbiomas_{ano}"
        tif_recortado = output_dir / f"mapbiomas_{ano}_recortado.tif"

        if tif_recortado.exists() and not forcar:
            logger.info(
                "[Grupo 6] MapBiomas %d: TIF recortado já existe — pulando clip. "
                "Use forcar=True para recalcular.",
                ano,
            )
        else:
            tile_path = _buscar_tile_mapbiomas(tile_dir, ano)
            logger.info("[Grupo 6] MapBiomas %d: clipping %s...", ano, tile_path.name)
            clip_raster(tile_path, limite_municipal, tif_recortado)

        logger.info("[Grupo 6] MapBiomas %d: calculando zonal stats categórico por setor...", ano)
        gdf_setores = ler_tabela_espacial(db_conn, "setores_censitarios")

        with rasterio.open(tif_recortado) as src:
            raster_crs = src.crs

        # nodata=0 suprime o NodataWarning e exclui pixels sem cobertura do denominador.
        # MapBiomas usa 0 para pixels fora da área de classificação (valor nunca usado como classe).
        raw_stats = zonal_stats(
            gdf_setores.to_crs(raster_crs),
            str(tif_recortado),
            categorical=True,
            nodata=0,
        )
        logger.info(
            "[Grupo 6] MapBiomas %d: zonal stats concluído — %d setores.",
            ano, len(raw_stats),
        )

        df_props = _props_por_setor(raw_stats)

        # Build output GeoDataFrame (sector code + proportions + geometry)
        col_cd = next(
            (c for c in gdf_setores.columns if c.upper() in ("CD_SETOR", "COD_SETOR")),
            None,
        )
        cols_base = ([col_cd] if col_cd else []) + ["geometry"]
        gdf_out = gdf_setores[cols_base].copy().reset_index(drop=True)
        if col_cd:
            gdf_out = gdf_out.rename(columns={col_cd: "cod_setor"})
        df_props = df_props.reset_index(drop=True)
        for col in df_props.columns:
            gdf_out[col] = df_props[col]

        salvar_geodataframe(db_conn, gdf_out, nome_tabela)
        camadas_salvas.append(nome_tabela)

        logger.info(
            "[Grupo 6] MapBiomas %d OK: %d setores | prop_urbano µ=%.3f | area_observada µ=%.3f",
            ano, len(gdf_out),
            gdf_out["prop_urbano"].mean(),
            gdf_out["area_observada_pct"].mean(),
        )

    return camadas_salvas


def _coletar_fcu(
    codigo_ibge: str,
    limite_municipal: gpd.GeoDataFrame,
    output_dir: Path,
    db_conn,
    fcu_cache_dir: Path,
    forcar: bool,
) -> list[str]:
    """Downloads FCU national shapefile and computes per-sector coverage."""
    camadas_salvas = []

    ext_dir  = fcu_cache_dir / "_fcu_extraido"
    zip_path = _download_fcu(fcu_cache_dir)

    if forcar and ext_dir.exists():
        import shutil
        shutil.rmtree(ext_dir)

    gdf_fcu = _ler_fcu_municipio(zip_path, codigo_ibge, limite_municipal, ext_dir)

    # fcu_municipio
    if len(gdf_fcu) > 0:
        (output_dir.parent / "precariedade").mkdir(parents=True, exist_ok=True)
        gdf_fcu.to_file(
            output_dir.parent / "precariedade" / f"fcu_{codigo_ibge}.gpkg",
            driver="GPKG",
        )
        salvar_geodataframe(db_conn, gdf_fcu, "fcu_municipio")
    else:
        # Cria tabela vazia com schema mínimo para que a Etapa 2 sempre encontre a tabela
        db_conn.execute("CREATE OR REPLACE TABLE fcu_municipio (geometry GEOMETRY)")
        logger.info("[Grupo 6] FCU: fcu_municipio criada vazia (nenhum polígono para este município).")
    camadas_salvas.append("fcu_municipio")

    # fcu_setor (sempre tem uma linha por setor)
    gdf_setores = ler_tabela_espacial(db_conn, "setores_censitarios")
    df_fcu_setor = _calcular_fcu_por_setor(gdf_fcu, gdf_setores)

    col_cd = next(
        (c for c in gdf_setores.columns if c.upper() in ("CD_SETOR", "COD_SETOR")),
        None,
    )
    cols_base = ([col_cd] if col_cd else []) + ["geometry"]
    gdf_fcu_setor = gdf_setores[cols_base].copy().reset_index(drop=True)
    if col_cd:
        gdf_fcu_setor = gdf_fcu_setor.rename(columns={col_cd: "cod_setor"})
    df_fcu_setor = df_fcu_setor.reset_index(drop=True)
    for col in df_fcu_setor.columns:
        gdf_fcu_setor[col] = df_fcu_setor[col]

    salvar_geodataframe(db_conn, gdf_fcu_setor, "fcu_setor")
    camadas_salvas.append("fcu_setor")

    n_com_fcu = int(gdf_fcu_setor["fcu_intersecta"].sum())
    logger.info(
        "[Grupo 6] FCU OK: %d polígonos FCU | %d/%d setores intersectam FCU",
        len(gdf_fcu), n_com_fcu, len(gdf_setores),
    )
    return camadas_salvas


# ---------------------------------------------------------------------------
# Função principal
# ---------------------------------------------------------------------------

def coletar_grupo6(
    codigo_ibge: str,
    limite_municipal: gpd.GeoDataFrame,
    output_dir: Path,
    db_conn,
    anos_mapbiomas: list[int] = None,
    tile_dir: Path = None,
    forcar: bool = False,
    **kwargs,
) -> dict:
    """
    Coleta uso e cobertura do solo (MapBiomas) e precariedade (FCU) para o município.

    Parâmetros
    ----------
    codigo_ibge : str
        Código IBGE de 7 dígitos.
    limite_municipal : gpd.GeoDataFrame
        Polígono do município — obrigatório.
    output_dir : Path
        Pasta de saída: data/raw/{municipio}/uso_solo/
    db_conn : duckdb.DuckDBPyConnection
        Conexão aberta com o DuckDB do município.
    anos_mapbiomas : list[int]
        Anos para processar. Padrão: [2022, 2024].
        Requer brazil_coverage_{ano}.tif em tile_dir.
    tile_dir : Path
        Pasta com os tiles globais (MapBiomas e VIIRS).
        Padrão: data/raw/tiles_globais/ relativo ao diretório de trabalho.
    forcar : bool
        Se True, reprocessa mesmo que arquivos já existam.
    **kwargs :
        fcu_cache_dir (Path): pasta para cache do ZIP FCU nacional.
        Se omitido, usa tile_dir/../cache_fcu.

    Retorna
    -------
    dict
        {"status": "ok"|"erro", "camadas": [...], "mensagem": "..."}

    Notas
    -----
    Os tiles MapBiomas (brazil_coverage_{ano}.tif) devem ser baixados manualmente
    em https://brasil.mapbiomas.org/ e salvos em tile_dir. O módulo não faz download
    automático — os arquivos são globais (~2 GB) e reutilizáveis para qualquer município.

    mapbiomas_{ano}_dominio_pnadc está previsto mas aguarda implementação de
    obter_geometria_dominio_pnadc() — pendente para sessão dedicada.
    """
    if limite_municipal is None:
        raise ValueError("limite_municipal é obrigatório para o Grupo 6.")

    if anos_mapbiomas is None:
        anos_mapbiomas = [2022, 2024]

    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    if tile_dir is None:
        tile_dir = Path("data/raw/tiles_globais")
    tile_dir = Path(tile_dir)

    fcu_cache_dir = Path(kwargs.get("fcu_cache_dir", tile_dir.parent / "cache_fcu"))

    camadas_salvas = []

    try:
        # --- MapBiomas ---
        camadas_mb = _coletar_mapbiomas(
            limite_municipal=limite_municipal,
            output_dir=output_dir,
            tile_dir=tile_dir,
            anos=anos_mapbiomas,
            db_conn=db_conn,
            forcar=forcar,
        )
        camadas_salvas.extend(camadas_mb)

        # --- FCU ---
        camadas_fcu = _coletar_fcu(
            codigo_ibge=codigo_ibge,
            limite_municipal=limite_municipal,
            output_dir=output_dir,
            db_conn=db_conn,
            fcu_cache_dir=fcu_cache_dir,
            forcar=forcar,
        )
        camadas_salvas.extend(camadas_fcu)

        return {
            "status": "ok",
            "camadas": camadas_salvas,
            "mensagem": f"{len(camadas_salvas)} camadas coletadas.",
        }

    except Exception as exc:
        logger.error("[Grupo 6] Erro: %s", exc, exc_info=True)
        return {
            "status": "erro",
            "camadas": camadas_salvas,
            "mensagem": str(exc),
        }
