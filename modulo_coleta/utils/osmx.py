"""
utils/osmx.py — Helpers para download de dados OSM via OSMnx.

Refatorado de: template_dados_IBGE_por_municipio.ipynb (célula baixar_eixos_osm).
Diferenças: logging, reprojeção garantida para EPSG:4674, aceita GeoDataFrame
ou Polygon shapely como entrada.

Funções públicas
----------------
baixar_eixos_osm(poligono, network_type) → gpd.GeoDataFrame
"""

from __future__ import annotations

import logging

import geopandas as gpd
from shapely.geometry.base import BaseGeometry

logger = logging.getLogger(__name__)


def baixar_eixos_osm(
    poligono: gpd.GeoDataFrame | BaseGeometry,
    network_type: str = "all",
) -> gpd.GeoDataFrame:
    """
    Baixa do OSM as 'ways' com tag highway dentro de um polígono e retorna
    as arestas do grafo viário como GeoDataFrame em EPSG:4674.

    Refatorado de: template_dados_IBGE_por_municipio.ipynb.

    Parâmetros
    ----------
    poligono : gpd.GeoDataFrame | shapely.Polygon
        Polígono de recorte. Se GeoDataFrame, usa a união de todas as geometrias.
    network_type : str
        Tipo de rede OSM. Opções: 'all', 'drive', 'walk', 'bike'.
        Padrão: 'all' (inclui todos os tipos de via).

    Retorna
    -------
    gpd.GeoDataFrame
        Arestas do grafo viário em EPSG:4674.
        Colunas principais: geometry (LineString), osmid, name, highway, length.

    Levanta
    -------
    ValueError
        Se o polígono for vazio ou inválido.
    """
    import osmnx as ox

    # Extrai geometria shapely
    if isinstance(poligono, gpd.GeoDataFrame):
        poly = poligono.to_crs("EPSG:4674").geometry.union_all()
    elif isinstance(poligono, gpd.GeoSeries):
        poly = poligono.to_crs("EPSG:4674").union_all()
    else:
        poly = poligono

    if poly is None or poly.is_empty:
        raise ValueError("Polígono de recorte está vazio.")

    ox.settings.use_cache = True
    ox.settings.log_console = False

    logger.info("Baixando eixos OSM (network_type=%s)...", network_type)
    G = ox.graph_from_polygon(poly, network_type=network_type, simplify=True)

    gdf_edges = ox.graph_to_gdfs(G, nodes=False)
    gdf_edges = gdf_edges.to_crs("EPSG:4674")

    # Reset index para remover MultiIndex (u, v, key)
    gdf_edges = gdf_edges.reset_index()

    # Normaliza colunas com valores mistos (escalar e lista) para string.
    # OSMnx pode gerar osmid, name, highway etc. como lista quando uma aresta
    # agrega múltiplas OSM ways. PyArrow rejeita colunas object com tipos mistos,
    # então a coluna inteira é convertida para string quando há alguma lista.
    for col in gdf_edges.columns:
        if col == "geometry":
            continue
        if gdf_edges[col].apply(lambda v: isinstance(v, list)).any():
            gdf_edges[col] = gdf_edges[col].astype(str)

    logger.info("Eixos OSM baixados: %d arestas", len(gdf_edges))
    return gdf_edges
