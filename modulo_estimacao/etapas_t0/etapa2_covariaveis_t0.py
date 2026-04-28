"""
etapa2_covariaveis_t0.py — Matriz de covariáveis territoriais por setor e H3 (ano-base).

Produz duas tabelas no DuckDB:
    covariaveis_setor_t0  — 1 linha por setor censitário
    covariaveis_h3_t0     — 1 linha por hexágono H3 (agrega setor → H3)

Covariáveis canônicas (lista completa em instrucoes_modulo_estimacao.md § 4):
    1  renda_resp_media             proxy_setor.renda_responsavel_media
    2  luminosidade_setor_mean      luminosidade_{ano_t0}.viirs_mean
    3  luminosidade_setor_std       luminosidade_{ano_t0}.viirs_std
    4  cnefe_residencial_densidade  enderecos_cnefe_residencial / AREA_KM2
    5  cnefe_naoresid_densidade     enderecos_cnefe_naoresidencial / AREA_KM2
    6  prop_urbano                  mapbiomas_{ano_t0}.prop_urbano
    7  prop_mosaico_uso             mapbiomas_{ano_t0}.prop_mosaico_uso
    8  prop_vegetacao               mapbiomas_{ano_t0}.prop_vegetacao
    9  fcu_intersecta               fcu_setor.fcu_intersecta
   10  fcu_area_pct                 fcu_setor.fcu_area_pct
   11  dist_centro_m                distância ao centróide do município (EPSG:5880)
   12  cnefe_densidade_buffer_500m  CNEFE residencial em buffer 500 m / area_buffer_km2

Dependências no DuckDB (pré-existentes):
    proxy_setor               — Etapa 1
    setores_censitarios       — Grupo 1
    limite_municipal          — Grupo 1
    luminosidade_{ano_t0}     — Grupo 4
    mapbiomas_{ano_t0}        — Grupo 6
    fcu_setor                 — Grupo 6
    enderecos_cnefe_residencial    — Grupo 3
    enderecos_cnefe_naoresidencial — Grupo 3
    grade_estatistica         — Grupo 1 (para n_domicilios_grade em covariaveis_h3_t0)

Notas de join:
    luminosidade, mapbiomas e fcu_setor usam CD_SETOR de 16 chars ('270030005000001P').
    proxy_setor usa cod_setor de 15 chars ('270030005000001').
    Normalização: LEFT(cd_setor_16chars, 15) = cod_setor_15chars.
"""

from __future__ import annotations

import logging

import geopandas as gpd
import numpy as np
import pandas as pd
import shapely.wkb as swkb

from ..utils.covariaveis_h3 import agregar_covariaveis_h3, setores_para_h3

logger = logging.getLogger(__name__)

_QUALIDADE_ACEITA = ("alta", "media")
_AREA_BUFFER_KM2 = np.pi * 0.5**2  # área do buffer 500 m em km²

_TABELAS_REQUERIDAS = [
    "proxy_setor",
    "setores_censitarios",
    "limite_municipal",
    "enderecos_cnefe_residencial",
    "enderecos_cnefe_naoresidencial",
    "fcu_setor",
]

_COLUNAS_NUMERICAS_H3 = [
    "renda_resp_media",
    "luminosidade_setor_mean",
    "luminosidade_setor_std",
    "cnefe_residencial_densidade",
    "cnefe_naoresid_densidade",
    "prop_urbano",
    "prop_mosaico_uso",
    "prop_vegetacao",
    "fcu_area_pct",
    "dist_centro_m",
    "cnefe_densidade_buffer_500m",
]

_COLUNAS_BOOL_H3 = ["fcu_intersecta"]


# ---------------------------------------------------------------------------
# Helpers internos
# ---------------------------------------------------------------------------

def _verificar_tabelas(db_conn, tabelas: list[str]) -> list[str]:
    presentes = {r[0] for r in db_conn.execute("SHOW TABLES").fetchall()}
    return [t for t in tabelas if t not in presentes]


def _load_gdf(db_conn, table: str, select_cols: str = "*", crs: str = "EPSG:4674") -> gpd.GeoDataFrame:
    """
    Carrega tabela do DuckDB como GeoDataFrame.

    Usa ST_AsWKB para garantir WKB bytes independente do tipo interno.
    """
    # Descobre qual coluna é geometry
    schema = db_conn.execute(f"DESCRIBE {table}").fetchdf()
    geom_col = schema.loc[schema["column_type"] == "GEOMETRY", "column_name"].values
    if len(geom_col) == 0:
        df = db_conn.execute(f"SELECT {select_cols} FROM {table}").df()
        return gpd.GeoDataFrame(df, crs=crs)

    geom_col = geom_col[0]
    # Monta SELECT substituindo geometry por ST_AsWKB(geometry) AS geometry
    if select_cols == "*":
        other_cols = [c for c in schema["column_name"] if c != geom_col]
        select_expr = ", ".join(other_cols) + f", ST_AsWKB({geom_col}) AS geometry"
    else:
        select_expr = select_cols.replace(geom_col, f"ST_AsWKB({geom_col}) AS {geom_col}")

    df = db_conn.execute(f"SELECT {select_expr} FROM {table}").df()
    geoms = df["geometry"].apply(lambda b: swkb.loads(bytes(b)) if b is not None else None)
    return gpd.GeoDataFrame(df.drop(columns=["geometry"]), geometry=geoms, crs=crs)


def _cnefe_densidade_por_setor(
    db_conn,
    tabela_cnefe: str,
) -> pd.DataFrame:
    """
    Conta pontos CNEFE (qualidade alta/media) dentro de cada setor via DuckDB spatial.

    Retorna DataFrame com (cod_setor, n_pontos, densidade_km2).
    """
    sql = f"""
    SELECT
        LEFT(s.CD_SETOR, 15) AS cod_setor,
        s.AREA_KM2,
        COUNT(c.geometry) AS n_pontos
    FROM setores_censitarios s
    LEFT JOIN {tabela_cnefe} c
        ON ST_Within(c.geometry, s.geometry)
        AND c.qualidade_geo IN ('alta', 'media')
    GROUP BY LEFT(s.CD_SETOR, 15), s.AREA_KM2
    """
    df = db_conn.execute(sql).df()
    df["densidade_km2"] = np.where(
        df["AREA_KM2"] > 0,
        df["n_pontos"] / df["AREA_KM2"],
        np.nan,
    )
    return df[["cod_setor", "n_pontos", "densidade_km2"]]


def _dist_centro_por_setor(db_conn) -> pd.DataFrame:
    """
    Distância (metros, EPSG:5880) do centróide de cada setor ao centróide do município.

    O centróide do município (limite_municipal) é usado como proxy do "centro urbano".
    """
    gdf_setores = _load_gdf(
        db_conn, "setores_censitarios",
        select_cols="CD_SETOR, geometry",
    )
    gdf_mun = _load_gdf(db_conn, "limite_municipal", select_cols="geometry")

    # Reproject para EPSG:5880 (Conica Conforme, Sul América — metros)
    gdf_setores_proj = gdf_setores.to_crs("EPSG:5880")
    gdf_mun_proj = gdf_mun.to_crs("EPSG:5880")

    centro = gdf_mun_proj.geometry.union_all().centroid

    setor_centroids = gdf_setores_proj.geometry.centroid
    dist = setor_centroids.distance(centro)

    return pd.DataFrame({
        "cod_setor": gdf_setores["CD_SETOR"].str[:15],
        "dist_centro_m": dist.values,
    })


def _cnefe_buffer_500m(db_conn) -> pd.DataFrame:
    """
    Densidade de endereços CNEFE residencial (qualidade alta/media) em buffer de 500 m.

    Unidade: pontos / km² (área do buffer = π * 0.5² ≈ 0.785 km²).
    """
    gdf_setores = _load_gdf(db_conn, "setores_censitarios", select_cols="CD_SETOR, geometry")
    gdf_cnefe = _load_gdf(
        db_conn,
        "enderecos_cnefe_residencial",
        select_cols="qualidade_geo, geometry",
    )

    # Filtra qualidade
    gdf_cnefe = gdf_cnefe[gdf_cnefe["qualidade_geo"].isin(_QUALIDADE_ACEITA)].copy()

    # Reproject para metros
    gdf_setores_proj = gdf_setores.to_crs("EPSG:5880")
    gdf_cnefe_proj = gdf_cnefe.to_crs("EPSG:5880")

    centroids = gdf_setores_proj.copy()
    centroids["geometry"] = gdf_setores_proj.geometry.centroid
    buffers = centroids.copy()
    buffers["geometry"] = centroids.geometry.buffer(500)

    # Spatial join: CNEFE dentro dos buffers
    joined = gpd.sjoin(gdf_cnefe_proj[["geometry"]], buffers[["CD_SETOR", "geometry"]], how="left", predicate="within")
    counts = joined.groupby("CD_SETOR").size().reset_index(name="n_buffer")

    result = buffers[["CD_SETOR"]].merge(counts, on="CD_SETOR", how="left")
    result["n_buffer"] = result["n_buffer"].fillna(0)
    result["cnefe_densidade_buffer_500m"] = result["n_buffer"] / _AREA_BUFFER_KM2

    return pd.DataFrame({
        "cod_setor": result["CD_SETOR"].str[:15],
        "cnefe_densidade_buffer_500m": result["cnefe_densidade_buffer_500m"].values,
    })


# ---------------------------------------------------------------------------
# Funções públicas
# ---------------------------------------------------------------------------

def extrair_covariaveis_setor_t0(
    codigo_ibge: str,
    ano_t0: int,
    db_conn,
    salvar: bool = True,
) -> dict:
    """
    Extrai e persiste a matriz de covariáveis territoriais por setor censitário.

    Parâmetros
    ----------
    codigo_ibge : str
        Código IBGE de 7 dígitos.
    ano_t0 : int
        Ano-base com Censo disponível (ex: 2022).
    db_conn : duckdb.DuckDBPyConnection
        Conexão aberta com o DuckDB do município.
    salvar : bool
        Se True, persiste `covariaveis_setor_t0` no DuckDB.

    Retorna
    -------
    dict
        {"status": "ok"|"erro", "n_setores": int, "mensagem": "..."}
    """
    logger.info("[Etapa 2] Extraindo covariaveis_setor_t0 — municipio %s, ano %d", codigo_ibge, ano_t0)

    # Verificar dependencias obrigatorias
    ausentes = _verificar_tabelas(db_conn, _TABELAS_REQUERIDAS)
    # luminosidade e mapbiomas dependem do ano
    for tabela_anual in [f"luminosidade_{ano_t0}", f"mapbiomas_{ano_t0}"]:
        if tabela_anual not in {r[0] for r in db_conn.execute("SHOW TABLES").fetchall()}:
            ausentes.append(tabela_anual)
    if ausentes:
        msg = f"Tabelas ausentes: {ausentes}"
        logger.error("[Etapa 2] %s", msg)
        return {"status": "erro", "mensagem": msg}

    # 1. Base: proxy_setor com renda
    logger.info("[Etapa 2] Carregando base (proxy_setor)")
    base = db_conn.execute(
        "SELECT cod_setor, renda_responsavel_media AS renda_resp_media FROM proxy_setor"
    ).df()

    # 2. Luminosidade
    logger.info("[Etapa 2] Luminosidade %d", ano_t0)
    lum = db_conn.execute(f"""
        SELECT LEFT(CD_SETOR, 15) AS cod_setor,
               viirs_mean AS luminosidade_setor_mean,
               viirs_std  AS luminosidade_setor_std
        FROM luminosidade_{ano_t0}
    """).df()
    base = base.merge(lum, on="cod_setor", how="left")

    # 3. MapBiomas
    logger.info("[Etapa 2] MapBiomas %d", ano_t0)
    mb = db_conn.execute(f"""
        SELECT LEFT(cod_setor, 15) AS cod_setor,
               prop_urbano, prop_mosaico_uso, prop_vegetacao
        FROM mapbiomas_{ano_t0}
    """).df()
    base = base.merge(mb, on="cod_setor", how="left")

    # 4. FCU
    logger.info("[Etapa 2] FCU")
    fcu = db_conn.execute("""
        SELECT LEFT(cod_setor, 15) AS cod_setor,
               fcu_intersecta, fcu_area_pct
        FROM fcu_setor
    """).df()
    base = base.merge(fcu, on="cod_setor", how="left")

    # 5. CNEFE residencial — densidade por setor (via DuckDB spatial)
    logger.info("[Etapa 2] CNEFE residencial densidade")
    cnefe_res = _cnefe_densidade_por_setor(db_conn, "enderecos_cnefe_residencial")
    cnefe_res = cnefe_res.rename(columns={"densidade_km2": "cnefe_residencial_densidade"})
    base = base.merge(cnefe_res[["cod_setor", "cnefe_residencial_densidade"]], on="cod_setor", how="left")

    # 6. CNEFE nao-residencial — densidade por setor
    logger.info("[Etapa 2] CNEFE nao-residencial densidade")
    cnefe_nao = _cnefe_densidade_por_setor(db_conn, "enderecos_cnefe_naoresidencial")
    cnefe_nao = cnefe_nao.rename(columns={"densidade_km2": "cnefe_naoresid_densidade"})
    base = base.merge(cnefe_nao[["cod_setor", "cnefe_naoresid_densidade"]], on="cod_setor", how="left")

    # 7. Distância ao centro
    logger.info("[Etapa 2] Distancia ao centro")
    dist = _dist_centro_por_setor(db_conn)
    base = base.merge(dist, on="cod_setor", how="left")

    # 8. CNEFE buffer 500 m
    logger.info("[Etapa 2] CNEFE buffer 500 m")
    buf = _cnefe_buffer_500m(db_conn)
    base = base.merge(buf, on="cod_setor", how="left")

    # 9. Geometria (para Etapa 4 e visualização)
    logger.info("[Etapa 2] Carregando geometria dos setores")
    geo = db_conn.execute(
        "SELECT LEFT(CD_SETOR, 15) AS cod_setor, ST_AsWKB(geometry) AS geometry FROM setores_censitarios"
    ).df()
    base = base.merge(geo, on="cod_setor", how="left")

    # Ordenar colunas
    colunas_saida = [
        "cod_setor",
        "renda_resp_media",
        "luminosidade_setor_mean", "luminosidade_setor_std",
        "cnefe_residencial_densidade", "cnefe_naoresid_densidade",
        "prop_urbano", "prop_mosaico_uso", "prop_vegetacao",
        "fcu_intersecta", "fcu_area_pct",
        "dist_centro_m",
        "cnefe_densidade_buffer_500m",
        "geometry",
    ]
    base = base[colunas_saida]

    n_setores = len(base)
    n_completos = base.drop(columns=["geometry"]).notna().all(axis=1).sum()
    logger.info(
        "[Etapa 2] %d setores | %d com todas as covariaveis preenchidas",
        n_setores, n_completos,
    )

    if salvar:
        db_conn.execute("DROP TABLE IF EXISTS covariaveis_setor_t0")
        db_conn.execute("CREATE TABLE covariaveis_setor_t0 AS SELECT * FROM base")
        logger.info("[Etapa 2] covariaveis_setor_t0 salvo: %d linhas", n_setores)

    return {
        "status": "ok",
        "n_setores": n_setores,
        "n_completos": int(n_completos),
        "mensagem": f"covariaveis_setor_t0: {n_setores} setores, {n_completos} completos.",
    }


def extrair_covariaveis_h3_t0(
    codigo_ibge: str,
    ano_t0: int,
    resolucao_h3: int,
    db_conn,
    salvar: bool = True,
) -> dict:
    """
    Agrega covariáveis do setor para a grade H3 via média ponderada por área.

    Requer que `covariaveis_setor_t0` já esteja no DuckDB (via extrair_covariaveis_setor_t0).

    Parâmetros
    ----------
    resolucao_h3 : int
        Resolução H3 alvo (ex: 8 ≈ 460 m de raio).

    Retorna
    -------
    dict
        {"status": "ok"|"erro", "n_hexagonos": int, "mensagem": "..."}
    """
    logger.info(
        "[Etapa 2-H3] Agregando para H3 res=%d — municipio %s", resolucao_h3, codigo_ibge
    )

    presentes = {r[0] for r in db_conn.execute("SHOW TABLES").fetchall()}
    if "covariaveis_setor_t0" not in presentes:
        msg = "Tabela covariaveis_setor_t0 ausente — execute extrair_covariaveis_setor_t0 primeiro."
        logger.error("[Etapa 2-H3] %s", msg)
        return {"status": "erro", "mensagem": msg}

    # Carrega covariaveis do setor
    covariaveis_setor = db_conn.execute(
        "SELECT * EXCLUDE geometry FROM covariaveis_setor_t0"
    ).df()

    # Carrega setores com geometria para mapeamento H3
    logger.info("[Etapa 2-H3] Carregando geometrias dos setores")
    gdf_setores = _load_gdf(
        db_conn, "setores_censitarios", select_cols="CD_SETOR, geometry"
    )
    gdf_setores["cod_setor"] = gdf_setores["CD_SETOR"].str[:15]
    # Filtra apenas setores com covariáveis
    gdf_setores = gdf_setores[
        gdf_setores["cod_setor"].isin(covariaveis_setor["cod_setor"])
    ].copy()

    # Mapeamento setor → H3
    logger.info("[Etapa 2-H3] Calculando mapeamento setor -> H3 (pode demorar ~30s)")
    mapeamento = setores_para_h3(gdf_setores, resolucao_h3)
    logger.info("[Etapa 2-H3] %d pares (setor, H3) encontrados", len(mapeamento))

    # Agrega covariáveis
    logger.info("[Etapa 2-H3] Agregando covariaveis")
    df_h3 = agregar_covariaveis_h3(
        covariaveis_setor,
        mapeamento,
        colunas_numericas=_COLUNAS_NUMERICAS_H3,
        colunas_bool=_COLUNAS_BOOL_H3,
    )
    df_h3["h3_resolucao"] = resolucao_h3

    # n_domicilios_grade: contagem de domicílios da grade IBGE 200m por H3
    if "grade_estatistica" in presentes:
        logger.info("[Etapa 2-H3] Agregando grade_estatistica -> H3")
        try:
            df_h3 = _agregar_grade_h3(db_conn, df_h3, resolucao_h3)
        except Exception as e:
            logger.warning("[Etapa 2-H3] grade_estatistica ignorada: %s", e)
            df_h3["n_domicilios_grade"] = np.nan
    else:
        logger.warning("[Etapa 2-H3] grade_estatistica ausente — n_domicilios_grade = NaN")
        df_h3["n_domicilios_grade"] = np.nan

    n_hex = len(df_h3)
    logger.info("[Etapa 2-H3] %d hexágonos H3 gerados", n_hex)

    if salvar:
        db_conn.execute("DROP TABLE IF EXISTS covariaveis_h3_t0")
        db_conn.execute("CREATE TABLE covariaveis_h3_t0 AS SELECT * FROM df_h3")
        logger.info("[Etapa 2-H3] covariaveis_h3_t0 salvo: %d linhas", n_hex)

        # Persiste mapeamento setor→H3 (usado na Etapa 5 para IPF)
        mapeamento_salvar = mapeamento.rename(columns={"cod_setor": "cod_setor"})[
            ["cod_setor", "h3_index", "peso_area"]
        ]
        db_conn.execute("DROP TABLE IF EXISTS mapeamento_h3_setor_t0")
        db_conn.execute("CREATE TABLE mapeamento_h3_setor_t0 AS SELECT * FROM mapeamento_salvar")
        logger.info("[Etapa 2-H3] mapeamento_h3_setor_t0 salvo: %d pares", len(mapeamento_salvar))

    return {
        "status": "ok",
        "n_hexagonos": n_hex,
        "mensagem": f"covariaveis_h3_t0: {n_hex} hexágonos H3 resolucao {resolucao_h3}.",
    }


def _agregar_grade_h3(db_conn, df_h3: pd.DataFrame, resolucao_h3: int) -> pd.DataFrame:
    """
    Conta domicílios da grade IBGE 200m dentro de cada H3 hexágono.

    Adiciona coluna n_domicilios_grade ao df_h3.
    """
    import h3
    from ..utils.covariaveis_h3 import _h3_from_point

    gdf_grade = _load_gdf(db_conn, "grade_estatistica", select_cols="TOTAL_DOM, geometry")
    # Centroides em CRS métrico para precisão; reconverte para lat/lng para H3
    centroids_metric = gdf_grade.to_crs("EPSG:5880").geometry.centroid
    centroids_geo = gpd.GeoSeries(centroids_metric, crs="EPSG:5880").to_crs("EPSG:4674")

    gdf_grade = gdf_grade.copy()
    gdf_grade["h3_index"] = [
        _h3_from_point(p.y, p.x, resolucao_h3) for p in centroids_geo
    ]

    grade_h3 = gdf_grade.groupby("h3_index")["TOTAL_DOM"].sum().reset_index()
    grade_h3 = grade_h3.rename(columns={"TOTAL_DOM": "n_domicilios_grade"})

    return df_h3.merge(grade_h3, on="h3_index", how="left")
