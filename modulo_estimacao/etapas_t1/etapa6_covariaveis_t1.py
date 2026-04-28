"""
etapa6_covariaveis_t1.py — Extração de covariáveis para o ano-corrente (t1).

Apenas as covariáveis com atualização anual disponível são recalculadas.
As demais ficam congeladas em t0 — limitação explícita do método.

Covariáveis recalculadas em t1
───────────────────────────────
    # | Covariável                   | Fonte t1                        | Atualização
    --|------------------------------|---------------------------------|------------
    2 | luminosidade_setor_mean (t1) | Grupo 4 (luminosidade_{ano_t1}) | anual (VIIRS)
    3 | luminosidade_setor_std  (t1) | Grupo 4 (luminosidade_{ano_t1}) | anual (VIIRS)
    6 | prop_urbano            (t1)  | Grupo 6 (mapbiomas_{ano_t1})    | anual (MapBiomas)
    7 | prop_mosaico_uso       (t1)  | Grupo 6 (mapbiomas_{ano_t1})    | anual (MapBiomas)
    8 | prop_vegetacao         (t1)  | Grupo 6 (mapbiomas_{ano_t1})    | anual (MapBiomas)

Covariáveis congeladas em t0 (sem atualização anual disponível no Brasil)
──────────────────────────────────────────────────────────────────────────
    renda_resp_media, cnefe_*_densidade, fcu_intersecta, fcu_area_pct,
    dist_centro_m, cnefe_densidade_buffer_500m.

    Essa é uma limitação metodológica documentada: o Censo é decenal, o CNEFE
    não tem atualização contínua, e a FCU é pontual (Censo 2022). A única forma
    de detectar expansão de favelas em t1 seria via análise de imagem de satélite
    adicional (não implementada nesta versão).

Variações calculadas (insumo da Etapa 7)
────────────────────────────────────────
    delta_lum_mean        = luminosidade_mean_t1 - luminosidade_mean_t0
    delta_lum_std         = luminosidade_std_t1  - luminosidade_std_t0
    delta_prop_urbano     = prop_urbano_t1 - prop_urbano_t0
    delta_prop_mosaico    = prop_mosaico_uso_t1 - prop_mosaico_uso_t0
    delta_prop_vegetacao  = prop_vegetacao_t1 - prop_vegetacao_t0
    flag_expansao         = (delta_prop_urbano > 0.05)  [bool]
        Sinaliza setores com expansão urbana relevante (> 5 pontos percentuais).
        Esses setores tendem a atrair população nova e aumentar o déficit qualitativo.

    delta_lum_dominio_pnadc = luminosidade_dominio_t1 - luminosidade_dominio_t0
        Variação de luminosidade no domínio amostral inteiro — usada como ancora
        macro do modelo temporal (Etapa 7).

Saída (tabelas no DuckDB)
─────────────────────────
    covariaveis_setor_t1:
        cod_setor, luminosidade_mean_t1, luminosidade_std_t1, prop_urbano_t1,
        prop_mosaico_uso_t1, prop_vegetacao_t1, geometry

    delta_covariaveis_setor:
        cod_setor, delta_lum_mean, delta_lum_std, delta_prop_urbano,
        delta_prop_mosaico, delta_prop_vegetacao, flag_expansao
"""

from __future__ import annotations

import logging

import numpy as np

logger = logging.getLogger(__name__)

_TABELAS_REQUERIDAS_FIXAS = ["covariaveis_setor_t0"]


# ---------------------------------------------------------------------------
# Helpers internos
# ---------------------------------------------------------------------------

def _verificar_tabelas(db_conn, tabelas: list[str]) -> list[str]:
    presentes = {r[0] for r in db_conn.execute("SHOW TABLES").fetchall()}
    return [t for t in tabelas if t not in presentes]


# ---------------------------------------------------------------------------
# Função pública
# ---------------------------------------------------------------------------

def extrair_covariaveis_setor_t1(
    codigo_ibge: str,
    ano_t0: int,
    ano_t1: int,
    db_conn,
    salvar: bool = True,
) -> dict:
    """
    Recalcula as covariáveis anuais para t1 e computa as variações em relação a t0.

    Parâmetros
    ----------
    codigo_ibge : str
        Código IBGE de 7 dígitos.
    ano_t0 : int
        Ano-base (ex: 2022). Usado para calcular deltas.
    ano_t1 : int
        Ano-corrente (ex: 2024). Determina quais tabelas MapBiomas e VIIRS usar.
    db_conn : duckdb.DuckDBPyConnection
        Deve conter: luminosidade_{ano_t0}, luminosidade_{ano_t1},
        mapbiomas_{ano_t0}, mapbiomas_{ano_t1}, covariaveis_setor_t0.
    salvar : bool
        Se True, persiste covariaveis_setor_t1 e delta_covariaveis_setor no DuckDB.

    Retorna
    -------
    dict
        {"status": "ok"|"erro", "camadas": [...], "n_setores": int,
         "n_expansao": int, "delta_lum_dominio_pnadc": float}
    """
    logger.info(
        "[Etapa 6] Extraindo covariaveis_setor_t1 — municipio %s, t0=%d, t1=%d",
        codigo_ibge, ano_t0, ano_t1,
    )

    # --- Verificar dependências ---
    tabelas_req = _TABELAS_REQUERIDAS_FIXAS + [
        f"luminosidade_{ano_t0}",
        f"luminosidade_{ano_t1}",
        f"mapbiomas_{ano_t0}",
        f"mapbiomas_{ano_t1}",
    ]
    ausentes = _verificar_tabelas(db_conn, tabelas_req)
    if ausentes:
        msg = f"Tabelas ausentes: {ausentes}"
        logger.error("[Etapa 6] %s", msg)
        return {"status": "erro", "mensagem": msg}

    # --- 1. Valores t0 de luminosidade e MapBiomas (base para deltas) ---
    logger.info("[Etapa 6] Carregando valores t0 de covariaveis_setor_t0")
    t0 = db_conn.execute("""
        SELECT cod_setor,
               luminosidade_setor_mean AS lum_mean_t0,
               luminosidade_setor_std  AS lum_std_t0,
               prop_urbano             AS prop_urbano_t0,
               prop_mosaico_uso        AS prop_mosaico_t0,
               prop_vegetacao          AS prop_veg_t0
        FROM covariaveis_setor_t0
    """).df()
    logger.info("[Etapa 6] %d setores carregados de t0", len(t0))

    # --- 2. Luminosidade t1 (VIIRS) ---
    logger.info("[Etapa 6] Luminosidade %d (t1)", ano_t1)
    lum_t1 = db_conn.execute(f"""
        SELECT LEFT(CD_SETOR, 15) AS cod_setor,
               viirs_mean AS luminosidade_mean_t1,
               viirs_std  AS luminosidade_std_t1
        FROM luminosidade_{ano_t1}
    """).df()

    # --- 3. MapBiomas t1 ---
    logger.info("[Etapa 6] MapBiomas %d (t1)", ano_t1)
    mb_t1 = db_conn.execute(f"""
        SELECT LEFT(cod_setor, 15) AS cod_setor,
               prop_urbano      AS prop_urbano_t1,
               prop_mosaico_uso AS prop_mosaico_uso_t1,
               prop_vegetacao   AS prop_vegetacao_t1
        FROM mapbiomas_{ano_t1}
    """).df()

    # --- 4. Geometria (herdada de t0) ---
    geo = db_conn.execute(
        "SELECT cod_setor, geometry FROM covariaveis_setor_t0"
    ).df()

    # --- 5. Montar DataFrame de trabalho ---
    df = t0.merge(lum_t1, on="cod_setor", how="left")
    df = df.merge(mb_t1, on="cod_setor", how="left")
    df = df.merge(geo, on="cod_setor", how="left")

    n_setores = len(df)
    n_lum_t1_ok = df["luminosidade_mean_t1"].notna().sum()
    n_mb_t1_ok = df["prop_urbano_t1"].notna().sum()
    logger.info(
        "[Etapa 6] %d setores | lum_t1 preenchidos: %d | MapBiomas_t1 preenchidos: %d",
        n_setores, n_lum_t1_ok, n_mb_t1_ok,
    )

    # --- 6. Calcular deltas ---
    df["delta_lum_mean"] = df["luminosidade_mean_t1"] - df["lum_mean_t0"]
    df["delta_lum_std"] = df["luminosidade_std_t1"] - df["lum_std_t0"]
    df["delta_prop_urbano"] = df["prop_urbano_t1"] - df["prop_urbano_t0"]
    df["delta_prop_mosaico"] = df["prop_mosaico_uso_t1"] - df["prop_mosaico_t0"]
    df["delta_prop_vegetacao"] = df["prop_vegetacao_t1"] - df["prop_veg_t0"]
    df["flag_expansao"] = df["delta_prop_urbano"] > 0.05

    n_expansao = int(df["flag_expansao"].sum())
    delta_lum_dominio = float(df["delta_lum_mean"].mean())
    logger.info(
        "[Etapa 6] Setores com flag_expansao (delta_urb > 5pp): %d/%d | "
        "delta_lum_dominio (media): %.4f",
        n_expansao, n_setores, delta_lum_dominio,
    )

    # --- 7. Montar tabelas de saída ---
    cov_t1 = df[[
        "cod_setor",
        "luminosidade_mean_t1", "luminosidade_std_t1",
        "prop_urbano_t1", "prop_mosaico_uso_t1", "prop_vegetacao_t1",
        "geometry",
    ]].copy()

    delta = df[[
        "cod_setor",
        "delta_lum_mean", "delta_lum_std",
        "delta_prop_urbano", "delta_prop_mosaico", "delta_prop_vegetacao",
        "flag_expansao",
    ]].copy()

    # --- 8. Persistir ---
    if salvar:
        db_conn.execute("DROP TABLE IF EXISTS covariaveis_setor_t1")
        db_conn.execute("CREATE TABLE covariaveis_setor_t1 AS SELECT * FROM cov_t1")
        logger.info("[Etapa 6] covariaveis_setor_t1 salvo: %d linhas", n_setores)

        db_conn.execute("DROP TABLE IF EXISTS delta_covariaveis_setor")
        db_conn.execute("CREATE TABLE delta_covariaveis_setor AS SELECT * FROM delta")
        logger.info("[Etapa 6] delta_covariaveis_setor salvo: %d linhas", n_setores)

    return {
        "status": "ok",
        "camadas": ["covariaveis_setor_t1", "delta_covariaveis_setor"],
        "n_setores": n_setores,
        "n_expansao": n_expansao,
        "delta_lum_dominio_pnadc": delta_lum_dominio,
    }
