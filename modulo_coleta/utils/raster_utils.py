"""
utils/raster_utils.py — Helpers para clip e estatísticas zonais em rasters.

Funções públicas
----------------
clip_raster(caminho_tif, geometria, destino, nodata)
    Recorta um raster pela geometria (GeoDataFrame ou Polygon) e salva em destino.
    Retorna Path do arquivo gerado.

zonal_stats_por_camada(caminho_tif, gdf, stats, prefixo)
    Calcula estatísticas zonais para cada feição de gdf sobre o raster.
    Retorna DataFrame com colunas {prefixo}_{stat} para cada estatística.

ler_tabela_espacial(conn, nome_tabela, crs)
    Lê uma tabela com geometria do DuckDB e devolve GeoDataFrame.
"""

from __future__ import annotations

import logging
from pathlib import Path

import geopandas as gpd
import pandas as pd

logger = logging.getLogger(__name__)


def clip_raster(
    caminho_tif: Path | str,
    geometria: gpd.GeoDataFrame,
    destino: Path | str,
    nodata: float | None = None,
) -> Path:
    """
    Recorta um raster GeoTIFF pela geometria do GeoDataFrame e salva o resultado.

    A geometria é reprojetada automaticamente para o CRS do raster antes do clip.
    Se o raster já tiver um valor nodata definido nos metadados, ele é preservado.
    O parâmetro `nodata` sobrescreve o valor do arquivo apenas se fornecido.

    Parâmetros
    ----------
    caminho_tif : Path | str
        Caminho para o GeoTIFF de entrada (pode ser um tile global grande).
    geometria : gpd.GeoDataFrame
        GeoDataFrame com a(s) geometria(s) de recorte (ex: limite municipal).
    destino : Path | str
        Caminho de saída para o GeoTIFF recortado.
    nodata : float | None
        Valor a tratar como nodata. Se None, usa o valor do arquivo de entrada.

    Retorna
    -------
    Path
        Caminho do arquivo GeoTIFF gerado.

    Levanta
    -------
    ValueError
        Se a geometria não intersectar o raster.
    """
    import rasterio
    from rasterio.mask import mask as rio_mask
    from shapely.geometry import mapping

    caminho_tif = Path(caminho_tif)
    destino = Path(destino)
    destino.parent.mkdir(parents=True, exist_ok=True)

    with rasterio.open(caminho_tif) as src:
        raster_crs = src.crs
        nodata_raster = src.nodata if nodata is None else nodata

        # Reproject geometry to raster CRS
        geom_proj = geometria.to_crs(raster_crs)
        shapes = [mapping(g) for g in geom_proj.geometry if g is not None]

        if not shapes:
            raise ValueError("Geometria de clip vazia após reprojeção para o CRS do raster.")

        try:
            out_image, out_transform = rio_mask(
                src, shapes, crop=True, nodata=nodata_raster
            )
        except Exception as exc:
            raise ValueError(
                f"Erro ao recortar raster — verifique se a geometria intersecta o tile.\n"
                f"CRS raster: {raster_crs} | Detalhe: {exc}"
            ) from exc

        out_meta = src.meta.copy()
        out_meta.update(
            {
                "driver": "GTiff",
                "height": out_image.shape[1],
                "width": out_image.shape[2],
                "transform": out_transform,
                "nodata": nodata_raster,
                "compress": "lzw",
            }
        )

    with rasterio.open(destino, "w", **out_meta) as dst:
        dst.write(out_image)

    size_mb = destino.stat().st_size / 1_048_576
    logger.info(
        "Raster recortado salvo em %s (%.1f MB, %dx%d px)",
        destino,
        size_mb,
        out_meta["width"],
        out_meta["height"],
    )
    return destino


def zonal_stats_por_camada(
    caminho_tif: Path | str,
    gdf: gpd.GeoDataFrame,
    stats: list[str] | None = None,
    prefixo: str = "viirs",
) -> pd.DataFrame:
    """
    Calcula estatísticas zonais de um raster para cada feição do GeoDataFrame.

    O GeoDataFrame é reprojetado para o CRS do raster antes do cálculo.
    As colunas do resultado são nomeadas como {prefixo}_{stat} para evitar
    conflitos com colunas existentes no GeoDataFrame de origem.

    Parâmetros
    ----------
    caminho_tif : Path | str
        Caminho para o GeoTIFF (preferencialmente já recortado para a área).
    gdf : gpd.GeoDataFrame
        GeoDataFrame com as zonas (ex: setores censitários, grade 200m).
    stats : list[str] | None
        Estatísticas a calcular. Padrão: ["mean", "median", "max", "std", "count"].
        Valores válidos: mean, median, max, min, std, sum, count, range, nodata.
    prefixo : str
        Prefixo adicionado aos nomes das colunas de estatística. Padrão: "viirs".

    Retorna
    -------
    pd.DataFrame
        DataFrame indexado igual ao gdf, com colunas {prefixo}_{stat}.
        Não contém coluna geometry — usar pd.concat com o gdf original para join.
    """
    from rasterstats import zonal_stats
    import rasterio

    if stats is None:
        stats = ["mean", "median", "max", "std", "count"]

    caminho_tif = Path(caminho_tif)

    with rasterio.open(caminho_tif) as src:
        raster_crs = src.crs
        nodata = src.nodata

    gdf_proj = gdf.to_crs(raster_crs)

    # Para o produto VIIRS average_masked, 0 representa ausência de dado
    # (pixel fora da área de cobertura), não luminosidade zero.
    # Quando o raster não define nodata nos metadados, usamos 0 explicitamente
    # para evitar que rasterstats assuma -999 (valor incorreto para VIIRS).
    nodata_efetivo = nodata if nodata is not None else 0

    logger.info(
        "Calculando zonal stats (%s) em %d feições (nodata=%s)...",
        ", ".join(stats),
        len(gdf_proj),
        nodata_efetivo,
    )

    result = zonal_stats(
        gdf_proj,
        str(caminho_tif),
        stats=stats,
        nodata=nodata_efetivo,
        all_touched=False,
    )

    df_stats = pd.DataFrame(result, index=gdf.index)
    df_stats.columns = [f"{prefixo}_{c}" for c in df_stats.columns]

    n_nodata = df_stats[f"{prefixo}_count"].isna().sum() if f"{prefixo}_count" in df_stats.columns else 0
    logger.info(
        "Zonal stats concluído: %d feições, %d sem dados.",
        len(df_stats),
        n_nodata,
    )
    return df_stats


def ler_tabela_espacial(
    conn,
    nome_tabela: str,
    crs: str = "EPSG:4674",
) -> gpd.GeoDataFrame:
    """
    Lê uma tabela com geometria do DuckDB e retorna GeoDataFrame.

    A geometria é armazenada como DuckDB GEOMETRY (WKB internamente). Esta
    função converte de volta para GeoDataFrame via ST_AsWKB + shapely.wkb.

    Parâmetros
    ----------
    conn : duckdb.DuckDBPyConnection
        Conexão aberta com o DuckDB.
    nome_tabela : str
        Nome da tabela a ler. Ex: "setores_censitarios".
    crs : str
        CRS a atribuir à geometria lida. Padrão: "EPSG:4674".

    Retorna
    -------
    gpd.GeoDataFrame
    """
    from shapely import wkb as shapely_wkb

    df = conn.execute(
        f"SELECT * EXCLUDE (geometry), ST_AsWKB(geometry) AS geometry FROM {nome_tabela}"
    ).fetchdf()

    geoms = df["geometry"].apply(lambda b: shapely_wkb.loads(bytes(b)))
    gdf = gpd.GeoDataFrame(df.drop(columns=["geometry"]), geometry=geoms, crs=crs)
    logger.info("Tabela '%s' lida do DuckDB: %d feições.", nome_tabela, len(gdf))
    return gdf
