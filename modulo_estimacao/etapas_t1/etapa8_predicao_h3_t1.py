"""
etapa8_predicao_h3_t1.py — Predição do déficit em H3 para o ano-corrente (t1).

Fluxo
─────
    1. Para cada hexágono H3, recuperar deficit_calibrado_h3_t0 (Etapa 5)
    2. Agregar delta_proxy_setor_predito (Etapa 7) do setor para H3 via
       média ponderada por área (mapeamento_h3_setor_t0)
    3. Aplicar variação:
           deficit_predito_t1_h = deficit_calibrado_t0_h + delta_proxy_h3 × n_dom_h
    4. Calibração final (se PNADc t1 disponível):
       - Se pnadc_deficit_componentes contém dados do ano_t1: ancora macro
       - Se não: mantém consistência interna com t0

Propagação de incerteza
────────────────────────
    IC do deficit_t1 via propagação simples das bandas do delta_proxy_h3:
        deficit_ic_lower_t1 = deficit_calibrado_t0 + delta_ic_lower_h3 × n_dom
        deficit_ic_upper_t1 = deficit_calibrado_t0 + delta_ic_upper_h3 × n_dom
    Inclui incerteza do modelo temporal (IC das árvores RF, Etapa 7).

Saída (tabelas no DuckDB)
─────────────────────────
    deficit_calibrado_h3_t1:
        h3_index (str), h3_resolucao (int),
        deficit_predito_t1 (float),
        deficit_ic_lower_t1 (float),
        deficit_ic_upper_t1 (float),
        deficit_calibrado_t1 (float),
        delta_aplicado (float),
        pnadc_t1_disponivel (bool),
        geometry (WKB)

    predicao_t1_metadados:
        ano_t0 (int), ano_t1 (int), resolucao_h3 (int),
        pnadc_t1_disponivel (bool), fator_calibracao_dominio_t1 (float),
        timestamp (str)
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _verificar_tabelas(db_conn, tabelas: list[str]) -> list[str]:
    presentes = {r[0] for r in db_conn.execute("SHOW TABLES").fetchall()}
    return [t for t in tabelas if t not in presentes]


def _agregar_delta_para_h3(delta_s: pd.DataFrame, mapping: pd.DataFrame) -> pd.DataFrame:
    """
    Agrega delta_proxy_setor_predito (nível setor) para H3 via média ponderada por área.

    Retorna DataFrame com (h3_index, delta_proxy_h3, delta_ic_lower_h3, delta_ic_upper_h3).
    """
    df = mapping.merge(delta_s, on="cod_setor", how="left")
    df = df.dropna(subset=["delta_proxy_predito"])

    if df.empty:
        logger.warning("[Etapa 8] Nenhum setor com delta_proxy disponível para agregação H3.")
        return pd.DataFrame(columns=["h3_index", "delta_proxy_h3", "delta_ic_lower_h3", "delta_ic_upper_h3"])

    df["w_delta"]    = df["delta_proxy_predito"]   * df["peso_area"]
    df["w_ic_lower"] = df["delta_proxy_ic_lower"]  * df["peso_area"]
    df["w_ic_upper"] = df["delta_proxy_ic_upper"]  * df["peso_area"]

    agg = df.groupby("h3_index").agg(
        soma_w=("peso_area", "sum"),
        soma_w_delta=("w_delta", "sum"),
        soma_w_ic_lower=("w_ic_lower", "sum"),
        soma_w_ic_upper=("w_ic_upper", "sum"),
    ).reset_index()

    agg["delta_proxy_h3"]    = agg["soma_w_delta"]    / agg["soma_w"]
    agg["delta_ic_lower_h3"] = agg["soma_w_ic_lower"] / agg["soma_w"]
    agg["delta_ic_upper_h3"] = agg["soma_w_ic_upper"] / agg["soma_w"]

    return agg[["h3_index", "delta_proxy_h3", "delta_ic_lower_h3", "delta_ic_upper_h3"]]


def _checar_pnadc_t1(db_conn, ano_t1: int) -> tuple[float, float, bool]:
    """
    Verifica se a PNADc disponível é do ano_t1.

    Retorna (total_pnadc_t1, cv_max, disponivel).
    """
    try:
        meta = db_conn.execute(
            "SELECT chave, valor FROM pnadc_metadados WHERE chave IN ('ano_referencia', 'ano')"
        ).df()
        if not meta.empty:
            ano_pnadc = int(meta.iloc[0]["valor"])
            if ano_pnadc != ano_t1:
                logger.info(
                    "[Etapa 8] PNADc no DuckDB é de %d, não %d — âncora t1 indisponível.",
                    ano_pnadc, ano_t1,
                )
                return np.nan, np.nan, False
    except Exception:
        pass  # sem metadados de ano — assume indisponível

    # Se chegou aqui, a PNADc pode ser do t1; tenta ler componentes
    try:
        df = db_conn.execute("""
            SELECT componente, total_estimado, cv
            FROM pnadc_deficit_componentes
            WHERE componente IN ('habitacao_precaria', 'coabitacao')
        """).df()
        if df.empty:
            return np.nan, np.nan, False
        total = float(df["total_estimado"].sum())
        cv_max = float(df["cv"].max())
        return total, cv_max, True
    except Exception as e:
        logger.warning("[Etapa 8] Erro ao ler pnadc_deficit_componentes: %s", e)
        return np.nan, np.nan, False


# ---------------------------------------------------------------------------
# Função pública
# ---------------------------------------------------------------------------

def predizer_h3_t1(
    codigo_ibge: str,
    ano_t0: int,
    ano_t1: int,
    resolucao_h3: int,
    db_conn,
    usar_ancora_pnadc: bool = True,
    output_dir=None,
) -> dict:
    """
    Aplica o modelo temporal (Etapa 7) em H3 e calibra com PNADc t1 se disponível.

    Parâmetros
    ----------
    codigo_ibge : str
        Código IBGE de 7 dígitos.
    ano_t0 : int
        Ano-base (ex: 2022).
    ano_t1 : int
        Ano-corrente (ex: 2024).
    resolucao_h3 : int
        Resolução H3 — deve ser a mesma usada na Etapa 4.
    db_conn : duckdb.DuckDBPyConnection
        Deve conter: deficit_calibrado_h3_t0 (Etapa 5),
        delta_proxy_setor_predito (Etapa 7),
        mapeamento_h3_setor_t0 (Etapa 2-H3).
    usar_ancora_pnadc : bool
        Se True (padrão), tenta calibrar com PNADc t1 se disponível.
    output_dir : Path | None
        Não utilizado nesta etapa (reservado para compatibilidade).

    Retorna
    -------
    dict
        {"status": "ok"|"erro", "camadas": [...], "n_hexagonos": int,
         "pnadc_t1_disponivel": bool, "deficit_calibrado_total": float}
    """
    logger.info(
        "[Etapa 8] Predição H3 t1 res=%d — municipio %s, t0=%d, t1=%d",
        resolucao_h3, codigo_ibge, ano_t0, ano_t1,
    )

    # --- Verificar dependências ---
    ausentes = _verificar_tabelas(db_conn, [
        "deficit_calibrado_h3_t0",
        "delta_proxy_setor_predito",
        "mapeamento_h3_setor_t0",
    ])
    if ausentes:
        msg = f"Tabelas ausentes: {ausentes}"
        logger.error("[Etapa 8] %s", msg)
        return {"status": "erro", "mensagem": msg}

    # --- 1. Baseline t0 por H3 ---
    logger.info("[Etapa 8] Carregando baseline t0")
    df_t0 = db_conn.execute("""
        SELECT h3_index, h3_resolucao,
               deficit_calibrado AS deficit_calibrado_t0,
               n_domicilios_grade,
               geometry
        FROM deficit_calibrado_h3_t0
    """).df()
    n_hex = len(df_t0)
    logger.info("[Etapa 8] %d hexágonos H3 carregados de t0", n_hex)

    # --- 2. Delta proxy por setor ---
    logger.info("[Etapa 8] Carregando delta_proxy por setor (Etapa 7)")
    delta_s = db_conn.execute("""
        SELECT cod_setor,
               delta_proxy_predito,
               delta_proxy_ic_lower,
               delta_proxy_ic_upper
        FROM delta_proxy_setor_predito
    """).df()

    # --- 3. Mapeamento setor→H3 ---
    mapping = db_conn.execute(
        "SELECT h3_index, cod_setor, peso_area FROM mapeamento_h3_setor_t0"
    ).df()

    # --- 4. Agregar delta para H3 ---
    logger.info("[Etapa 8] Agregando delta_proxy setor → H3")
    delta_h3 = _agregar_delta_para_h3(delta_s, mapping)
    n_h3_com_delta = len(delta_h3)
    logger.info(
        "[Etapa 8] %d/%d hexágonos com delta_proxy agregado",
        n_h3_com_delta, n_hex,
    )

    # --- 5. Merge e calcular predição t1 ---
    df = df_t0.merge(delta_h3, on="h3_index", how="left")

    # Δdef_h3 = delta_proxy_h3 × n_domicilios_grade
    df["delta_aplicado"]    = df["delta_proxy_h3"]    * df["n_domicilios_grade"]
    df["delta_ic_lower_abs"] = df["delta_ic_lower_h3"] * df["n_domicilios_grade"]
    df["delta_ic_upper_abs"] = df["delta_ic_upper_h3"] * df["n_domicilios_grade"]

    # deficit_t1 = baseline_t0 + delta (clipado a 0)
    df["deficit_predito_t1"]  = (df["deficit_calibrado_t0"].fillna(0) + df["delta_aplicado"].fillna(0)).clip(lower=0)
    df["deficit_ic_lower_t1"] = (df["deficit_calibrado_t0"].fillna(0) + df["delta_ic_lower_abs"].fillna(0)).clip(lower=0)
    df["deficit_ic_upper_t1"] = (df["deficit_calibrado_t0"].fillna(0) + df["delta_ic_upper_abs"].fillna(0)).clip(lower=0)

    soma_predito = float(df["deficit_predito_t1"].sum())
    logger.info(
        "[Etapa 8] deficit_predito_t1 total=%.1f (t0=%.1f, delta=%.1f)",
        soma_predito,
        float(df["deficit_calibrado_t0"].sum()),
        float(df["delta_aplicado"].fillna(0).sum()),
    )

    # --- 6. Calibração PNADc t1 (se disponível) ---
    pnadc_total, pnadc_cv, pnadc_disponivel = _checar_pnadc_t1(db_conn, ano_t1)
    fator_dominio_t1 = 1.0

    if usar_ancora_pnadc and pnadc_disponivel and not np.isnan(pnadc_total):
        fator_dominio_t1 = pnadc_total / soma_predito if soma_predito > 0 else 1.0
        df["deficit_calibrado_t1"] = df["deficit_predito_t1"] * fator_dominio_t1
        logger.info(
            "[Etapa 8] Âncora PNADc t1 aplicada: fator=%.4f (PNADc=%.1f, predito=%.1f)",
            fator_dominio_t1, pnadc_total, soma_predito,
        )
    else:
        if not pnadc_disponivel:
            logger.info("[Etapa 8] PNADc t1 indisponível — deficit_calibrado_t1 = predito sem âncora macro")
        df["deficit_calibrado_t1"] = df["deficit_predito_t1"]

    deficit_calibrado_total = float(df["deficit_calibrado_t1"].sum())
    logger.info("[Etapa 8] deficit_calibrado_t1 total=%.1f", deficit_calibrado_total)

    # --- 7. Montar tabela de saída ---
    resultado = df[[
        "h3_index",
        "h3_resolucao",
        "deficit_predito_t1",
        "deficit_ic_lower_t1",
        "deficit_ic_upper_t1",
        "deficit_calibrado_t1",
        "delta_aplicado",
        "geometry",
    ]].copy()
    resultado["pnadc_t1_disponivel"] = pnadc_disponivel

    # --- 8. Persistir ---
    db_conn.execute("DROP TABLE IF EXISTS deficit_calibrado_h3_t1")
    db_conn.execute("CREATE TABLE deficit_calibrado_h3_t1 AS SELECT * FROM resultado")
    logger.info("[Etapa 8] deficit_calibrado_h3_t1 salvo: %d linhas", len(resultado))

    meta = pd.DataFrame([{
        "ano_t0": ano_t0,
        "ano_t1": ano_t1,
        "resolucao_h3": resolucao_h3,
        "n_hexagonos": n_hex,
        "n_com_delta": n_h3_com_delta,
        "deficit_t0_total": round(float(df["deficit_calibrado_t0"].sum()), 1),
        "deficit_predito_t1_total": round(soma_predito, 1),
        "deficit_calibrado_t1_total": round(deficit_calibrado_total, 1),
        "pnadc_t1_disponivel": pnadc_disponivel,
        "fator_calibracao_dominio_t1": round(fator_dominio_t1, 4),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }])
    db_conn.execute("DROP TABLE IF EXISTS predicao_t1_metadados")
    db_conn.execute("CREATE TABLE predicao_t1_metadados AS SELECT * FROM meta")
    logger.info("[Etapa 8] predicao_t1_metadados salvo")

    return {
        "status": "ok",
        "camadas": ["deficit_calibrado_h3_t1", "predicao_t1_metadados"],
        "n_hexagonos": n_hex,
        "n_com_delta": n_h3_com_delta,
        "pnadc_t1_disponivel": pnadc_disponivel,
        "deficit_t0_total": round(float(df["deficit_calibrado_t0"].sum()), 1),
        "deficit_predito_t1_total": round(soma_predito, 1),
        "deficit_calibrado_t1_total": round(deficit_calibrado_total, 1),
    }
