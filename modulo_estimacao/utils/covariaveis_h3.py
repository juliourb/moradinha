"""
covariaveis_h3.py — Mapeamento setor → H3 com pesos de área e agregação.

Funções compartilhadas pelas Etapas 2 e 6.

A agregação setor → H3 usa média ponderada pela área de interseção:
    - Um hexágono H3 pode conter partes de múltiplos setores
    - Cada setor contribui proporcionalmente à sua área dentro do H3

Compatibilidade h3-py:
    - v4.x: latlng_to_cell, geo_to_cells, grid_disk, cell_to_boundary
    - v3.x: geo_to_h3, polyfill_geojson, k_ring, h3_to_geo_boundary
    Tenta v4 primeiro; cai no v3 via except AttributeError.
"""

from __future__ import annotations

import logging

import geopandas as gpd
import numpy as np
import pandas as pd
import shapely.geometry as sg

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Wrappers h3-py agnósticos de versão
# ---------------------------------------------------------------------------

def _h3_polyfill(geojson_dict: dict, resolucao: int) -> set[str]:
    """Retorna células H3 que cobrem o polígono GeoJSON."""
    import h3
    try:
        return set(h3.geo_to_cells(geojson_dict, resolucao))  # v4
    except AttributeError:
        return set(h3.polyfill_geojson(geojson_dict, resolucao))  # v3


def _h3_disk(cell: str, k: int) -> set[str]:
    """Retorna k-ring de células em torno de cell."""
    import h3
    try:
        return set(h3.grid_disk(cell, k))  # v4
    except AttributeError:
        return set(h3.k_ring(cell, k))  # v3


def _h3_boundary(cell: str) -> list[tuple[float, float]]:
    """Retorna lista de (lat, lng) do contorno da célula H3."""
    import h3
    try:
        return h3.cell_to_boundary(cell)  # v4 — retorna list[(lat,lng)]
    except AttributeError:
        return h3.h3_to_geo_boundary(cell)  # v3


def _h3_from_point(lat: float, lng: float, resolucao: int) -> str:
    """Retorna célula H3 para um ponto (lat, lng)."""
    import h3
    try:
        return h3.latlng_to_cell(lat, lng, resolucao)  # v4
    except AttributeError:
        return h3.geo_to_h3(lat, lng, resolucao)  # v3


def _cell_poly(cell: str) -> sg.Polygon:
    """Converte uma célula H3 em polígono shapely (EPSG:4674 graus)."""
    boundary = _h3_boundary(cell)
    coords = [(lng, lat) for lat, lng in boundary]
    return sg.Polygon(coords)


# ---------------------------------------------------------------------------
# Mapeamento setor → H3
# ---------------------------------------------------------------------------

def setores_para_h3(
    gdf_setores: gpd.GeoDataFrame,
    resolucao: int,
) -> pd.DataFrame:
    """
    Cria mapeamento setor → H3 com peso de área de interseção.

    Parâmetros
    ----------
    gdf_setores : gpd.GeoDataFrame
        Setores com colunas cod_setor e geometry em EPSG:4674.
    resolucao : int
        Resolução H3 (ex: 8 ≈ 460 m de raio).

    Retorna
    -------
    pd.DataFrame
        Colunas: cod_setor, h3_index, inter_area, peso_area.
        Uma linha por par (setor, hexágono) que se intersectam.
    """
    records = []
    n_setores = len(gdf_setores)

    for i, (_, row) in enumerate(gdf_setores.iterrows()):
        geom = row.geometry
        if geom is None or geom.is_empty:
            continue
        cod = row["cod_setor"]

        geojson = sg.mapping(geom)

        # Células H3 que cobrem o polígono
        cells = _h3_polyfill(geojson, resolucao)

        # Adiciona vizinhos para capturar sobreposições nas bordas
        border_cells: set[str] = set()
        for cell in cells:
            border_cells.update(_h3_disk(cell, 1))
        cells = cells | border_cells

        # Fallback para setores muito pequenos (polyfill retornou vazio)
        if not cells:
            c = geom.centroid
            cells = {_h3_from_point(c.y, c.x, resolucao)}

        for cell in cells:
            cell_poly = _cell_poly(cell)
            try:
                inter = geom.intersection(cell_poly)
            except Exception:
                continue
            if inter.is_empty or inter.area <= 0:
                continue
            records.append({"cod_setor": cod, "h3_index": cell, "inter_area": inter.area})

        if (i + 1) % 100 == 0:
            logger.debug("setores_para_h3: %d/%d setores processados", i + 1, n_setores)

    if not records:
        return pd.DataFrame(columns=["cod_setor", "h3_index", "inter_area", "peso_area"])

    df = pd.DataFrame(records)
    total_por_setor = df.groupby("cod_setor")["inter_area"].transform("sum")
    df["peso_area"] = df["inter_area"] / total_por_setor
    return df


# ---------------------------------------------------------------------------
# Agregação covariáveis setor → H3
# ---------------------------------------------------------------------------

def agregar_covariaveis_h3(
    covariaveis_setor: pd.DataFrame,
    mapeamento_h3: pd.DataFrame,
    colunas_numericas: list[str],
    colunas_bool: list[str] | None = None,
) -> pd.DataFrame:
    """
    Agrega covariáveis do setor para H3 via média ponderada por área.

    Parâmetros
    ----------
    covariaveis_setor : pd.DataFrame
        Tabela covariaveis_setor_t0 (ou t1) com coluna cod_setor.
    mapeamento_h3 : pd.DataFrame
        Saída de setores_para_h3() com colunas cod_setor, h3_index, peso_area.
    colunas_numericas : list[str]
        Covariáveis a agregar por média ponderada.
    colunas_bool : list[str] | None
        Covariáveis booleanas: agregação por OR lógico.

    Retorna
    -------
    pd.DataFrame
        Uma linha por h3_index com todas as covariáveis agregadas.
    """
    colunas_bool = colunas_bool or []
    df = mapeamento_h3[["cod_setor", "h3_index", "peso_area"]].merge(
        covariaveis_setor, on="cod_setor", how="left"
    )

    result_parts = []

    for col in colunas_numericas:
        mask = df[col].notna()
        num = (
            (df.loc[mask, col] * df.loc[mask, "peso_area"])
            .groupby(df.loc[mask, "h3_index"])
            .sum()
        )
        den = df.loc[mask, "peso_area"].groupby(df.loc[mask, "h3_index"]).sum()
        result_parts.append((num / den).rename(col))

    for col in colunas_bool:
        result_parts.append(df.groupby("h3_index")[col].any().rename(col))

    if not result_parts:
        return pd.DataFrame({"h3_index": df["h3_index"].unique()})

    return pd.concat(result_parts, axis=1).reset_index().rename(columns={"index": "h3_index"})
