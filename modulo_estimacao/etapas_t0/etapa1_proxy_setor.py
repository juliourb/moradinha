"""
etapa1_proxy_setor.py — Índice composto de carências habitacionais por setor censitário.

Calcula o proxy_carencias_setor a partir dos agregados do Universo do Censo 2022
disponíveis no DuckDB do município. Substitui o proxy de componentes FJP individuais
enquanto os agregados da Amostra (A1-rústicos, B2-adensamento, C-ônus) não são
publicados pelo IBGE por setor.

Fórmula (pesos iguais — definitivos na Etapa 3):
    proxy_carencias_setor = (1/6) * (
          prop_improvisados
        + prop_comodos_cortico
        + prop_sem_banheiro
        + prop_sem_agua_rede        (= 1 - prop_agua_rede_geral)
        + prop_sem_esgoto_rede      (= 1 - prop_esgoto_rede_geral)
        + prop_sem_lixo_coletado    (= 1 - prop_lixo_coletado)
    )

Variáveis Censo 2022 usadas (Universo — agregados por setor):
    V00001  total DPP ocupados            censo_domicilio01
    V00002  improvisados                  censo_domicilio01
    V00050  cômodos/cortiço               censo_domicilio01
    V00238  sem banheiro nem sanitário    censo_domicilio02
    V00111  com água via rede geral       censo_domicilio02
    V00309  com esgoto via rede geral     censo_domicilio02
    V00397  com lixo coletado             censo_domicilio02
    V06004  renda média do responsável    censo_responsavel01 (covariável)

Saída — tabela `proxy_setor` no DuckDB:
    cod_setor                str    — Cod_setor (15 dígitos)
    n_dom_total              int    — V00001
    n_improvisados           int    — V00002
    n_comodos_cortico        int    — V00050
    n_sem_banheiro           int    — V00238
    n_com_agua_rede          int    — V00111
    n_com_esgoto_rede        int    — V00309
    n_com_lixo_coletado      int    — V00397
    prop_improvisados        float
    prop_comodos_cortico     float
    prop_sem_banheiro        float
    prop_sem_agua_rede       float
    prop_sem_esgoto_rede     float
    prop_sem_lixo_coletado   float
    proxy_carencias_setor    float  — variável-alvo da Etapa 3
    renda_responsavel_media  float  — covariável (feature) da Etapa 3
    geometry                 BLOB   — geometria WKB do setor

Dependências no DuckDB:
    censo_domicilio01, censo_domicilio02, censo_responsavel01, setores_censitarios

Referência: FJP (2021). Déficit Habitacional no Brasil 2016-2019. Cap. 2.
    IBGE (2024). Censo Demografico 2022: Nota Metodologica n.06.
"""

from __future__ import annotations

import logging

import numpy as np
import pandas as pd

from ..utils.deficit_fjp_proxy import (
    proporcoes_carencias_setor,
    proxy_carencias_igual,
    to_numeric_br,
)

logger = logging.getLogger(__name__)

_TABELAS_REQUERIDAS = [
    "censo_domicilio01",
    "censo_domicilio02",
    "censo_responsavel01",
    "setores_censitarios",
]


def _verificar_tabelas(db_conn) -> list[str]:
    """Retorna lista de tabelas requeridas que estão ausentes no DuckDB."""
    presentes = {r[0] for r in db_conn.execute("SHOW TABLES").fetchall()}
    return [t for t in _TABELAS_REQUERIDAS if t not in presentes]


def _carregar_renda(db_conn) -> pd.DataFrame:
    """
    Carrega renda_responsavel_media de censo_responsavel01.

    Usa V06004 (media pre-calculada pelo IBGE). Fallback: V06005 / V06001.
    Valores em formato BR (virgula decimal).
    """
    df = db_conn.execute(
        "SELECT Cod_setor, V06001, V06004, V06005 FROM censo_responsavel01"
    ).df()
    df = df.rename(columns={"Cod_setor": "cod_setor"})

    v06004 = to_numeric_br(df["V06004"])

    # Fallback: recalcular a media quando V06004 eh NaN
    v06001 = to_numeric_br(df["V06001"])
    v06005 = to_numeric_br(df["V06005"])
    v06004_calc = np.where(v06001 > 0, v06005 / v06001, np.nan)

    df["renda_responsavel_media"] = np.where(
        v06004.notna(), v06004, v06004_calc
    )
    return df[["cod_setor", "renda_responsavel_media"]]


def _carregar_geometria(db_conn) -> pd.DataFrame:
    """
    Carrega CD_SETOR e geometry de setores_censitarios.

    CD_SETOR tem 16 caracteres (15 digitos + sufixo de especie, ex: 'P').
    Trunca para 15 digitos para compatibilizar com Cod_setor das tabelas censo.
    """
    df = db_conn.execute(
        "SELECT CD_SETOR, ST_AsWKB(geometry) AS geometry FROM setores_censitarios"
    ).df()
    df["cod_setor"] = df["CD_SETOR"].str[:15]
    return df[["cod_setor", "geometry"]]


def calcular_proxy_setor(
    codigo_ibge: str,
    db_conn,
    salvar: bool = True,
) -> dict:
    """
    Calcula o proxy_carencias_setor e persiste em `proxy_setor` no DuckDB.

    Parâmetros
    ----------
    codigo_ibge : str
        Codigo IBGE de 7 digitos. Ex: "2700300".
    db_conn : duckdb.DuckDBPyConnection
        Conexao aberta com o DuckDB do municipio.
    salvar : bool
        Se True, persiste `proxy_setor` no DuckDB. Padrao True.

    Retorna
    -------
    dict
        {"status": "ok"|"erro", "n_setores": int, "n_sem_geometria": int,
         "n_dom_total": int, "mensagem": "..."}
    """
    logger.info("[Etapa 1] Iniciando proxy_setor — municipio %s", codigo_ibge)

    # 1. Verificar dependencias
    ausentes = _verificar_tabelas(db_conn)
    if ausentes:
        msg = f"Tabelas ausentes no DuckDB: {ausentes}"
        logger.error("[Etapa 1] %s", msg)
        return {"status": "erro", "mensagem": msg}

    # 2. Carregar tabelas censo
    logger.info("[Etapa 1] Carregando tabelas censo")
    d1 = db_conn.execute(
        "SELECT Cod_setor, V00001, V00002, V00050 FROM censo_domicilio01"
    ).df()
    d2 = db_conn.execute(
        "SELECT Cod_setor, V00238, V00111, V00309, V00397 FROM censo_domicilio02"
    ).df()

    # 3. Calcular proporcoes de carencia
    logger.info("[Etapa 1] Calculando proporcoes de carencia")
    df = proporcoes_carencias_setor(d1, d2)

    # 4. Calcular proxy_carencias_setor (pesos iguais — Etapa 3 ajusta)
    df["proxy_carencias_setor"] = proxy_carencias_igual(df)

    # 5. Converter contagens brutas para inteiro (NaN -> -1 para indicar supressao)
    int_cols = [
        "n_dom_total", "n_improvisados", "n_comodos_cortico",
        "n_sem_banheiro", "n_com_agua_rede", "n_com_esgoto_rede", "n_com_lixo_coletado",
    ]
    for col in int_cols:
        df[col] = df[col].fillna(-1).astype(int)

    # 6. Renda do responsavel
    logger.info("[Etapa 1] Carregando renda do responsavel")
    renda = _carregar_renda(db_conn)
    df = df.merge(renda, on="cod_setor", how="left")

    # 7. Geometria dos setores
    logger.info("[Etapa 1] Juntando geometria")
    geo = _carregar_geometria(db_conn)
    n_antes = len(df)
    df = df.merge(geo, on="cod_setor", how="left")
    n_sem_geo = df["geometry"].isna().sum()
    if n_sem_geo > 0:
        logger.warning(
            "[Etapa 1] %d setores sem geometria (%.1f%% do total)",
            n_sem_geo, 100 * n_sem_geo / len(df),
        )

    # 8. Ordenar colunas canonicas
    colunas_saida = [
        "cod_setor",
        "n_dom_total", "n_improvisados", "n_comodos_cortico",
        "n_sem_banheiro", "n_com_agua_rede", "n_com_esgoto_rede", "n_com_lixo_coletado",
        "prop_improvisados", "prop_comodos_cortico", "prop_sem_banheiro",
        "prop_sem_agua_rede", "prop_sem_esgoto_rede", "prop_sem_lixo_coletado",
        "proxy_carencias_setor",
        "renda_responsavel_media",
        "geometry",
    ]
    df = df[colunas_saida]

    n_setores = len(df)
    n_dom_total = int(df.loc[df["n_dom_total"] > 0, "n_dom_total"].sum())
    logger.info(
        "[Etapa 1] %d setores | %d domicilios totais | proxy medio=%.4f",
        n_setores,
        n_dom_total,
        df["proxy_carencias_setor"].mean(),
    )

    # 9. Persistir no DuckDB
    if salvar:
        logger.info("[Etapa 1] Salvando proxy_setor no DuckDB")
        db_conn.execute("DROP TABLE IF EXISTS proxy_setor")
        db_conn.execute("CREATE TABLE proxy_setor AS SELECT * FROM df")
        logger.info("[Etapa 1] proxy_setor salvo: %d linhas", n_setores)

    return {
        "status": "ok",
        "n_setores": n_setores,
        "n_sem_geometria": int(n_sem_geo),
        "n_dom_total": n_dom_total,
        "proxy_medio": round(float(df["proxy_carencias_setor"].mean()), 6),
        "mensagem": f"proxy_setor calculado: {n_setores} setores, {n_dom_total} domicilios.",
    }
