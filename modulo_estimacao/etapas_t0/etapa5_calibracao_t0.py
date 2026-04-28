"""
etapa5_calibracao_t0.py — Calibração das predições H3 para totais conhecidos (t0).

Objetivo
────────
Ajustar as predições H3 da Etapa 4 para que sejam consistentes com dois totais
conhecidos em diferentes escalas:

    1. Total do setor (Censo 2022, Etapa 1): a soma dos hexágonos dentro de cada
       setor deve igualar proxy_total_sem_onus do setor.

    2. Total do domínio PNADc (Grupo 5, Etapa 5 do modulo_coleta): a soma dos
       setores dentro do domínio amostral deve igualar deficit_total_pnadc.

Método: calibração direta em 2 passos
──────────────────────────────────────
    Passo 1 — ancoragem intraurbana (Censo 2022, precisão fina):
        Para cada setor s com H3 válidos:
            share_h = deficit_predito_h / soma_predita_s   (proporção dentro do setor)
            deficit_cal1_h = share_h × alvo_s              (distribui alvo censitário)
        Preserva a distribuição relativa entre H3 dentro de cada setor.
        Setores sem H3 com domicílios não participam do Passo 1.

    Passo 2 — ancoragem macro (PNADc, controle externo):
        fator_pnadc = total_pnadc / soma(deficit_cal1)
        deficit_calibrado_h = deficit_cal1_h × fator_pnadc
        Um único fator global que ancora o total ao valor da PNADc.

    Nota metodológica: O Passo 2 redistribui o total PNADc proporcionalmente aos
    valores do Passo 1, que preservam o ranqueamento do Censo. O resultado final
    reflete a distribuição intraurbana do Censo ancorada ao total macro da PNADc.
    A incerteza da PNADc (CV elevado para municípios pequenos) é registrada nos
    metadados e pode justificar desabilitar o Passo 2 com usar_ancora_pnadc=False.

Saída (tabelas no DuckDB)
─────────────────────────
    deficit_calibrado_h3_t0:
        h3_index (str), h3_resolucao (int),
        proxy_predito (float), proxy_ic_lower (float), proxy_ic_upper (float),
        n_domicilios_grade (int),
        deficit_predito (float),          — antes da calibração (Etapa 4)
        deficit_calibrado (float),        — após calibração Passo 1+2
        fator_calibracao_setor (float),   — fator Passo 1 (distribuição Censo)
        fator_calibracao_dominio (float), — fator Passo 2 (âncora PNADc)
        cod_setor_dominante (str),        — setor dominante do H3 (maior área)
        geometry (WKB)

    calibracao_metadados_t0:
        chave (str), valor (str) — parâmetros e resultados do processo
"""

from __future__ import annotations

import logging

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

_COMPONENTES_PNADC_SEM_ONUS = ("habitacao_precaria", "coabitacao")


def _total_pnadc_sem_onus(db_conn) -> tuple[float, float, bool]:
    """
    Retorna (total_estimado, cv_max, cv_alto) para habitacao_precaria + coabitacao.

    cv_alto=True indica que pelo menos um componente tem CV > 33%.
    """
    comps = ", ".join(f"'{c}'" for c in _COMPONENTES_PNADC_SEM_ONUS)
    df = db_conn.execute(f"""
        SELECT componente, total_estimado, cv, cv_alto
        FROM pnadc_deficit_componentes
        WHERE componente IN ({comps})
    """).df()

    if df.empty:
        return np.nan, np.nan, True

    total = float(df["total_estimado"].sum())
    cv_max = float(df["cv"].max())
    cv_alto = bool(df["cv_alto"].any())
    return total, cv_max, cv_alto


def calibrar_h3_t0(
    codigo_ibge: str,
    db_conn,
    tolerancia: float = 1e-4,
    max_iter: int = 50,
    usar_ancora_pnadc: bool = True,
) -> dict:
    """
    Calibra as predições H3 via dois passos: distribuição Censo + âncora PNADc.

    Parâmetros
    ----------
    codigo_ibge : str
        Código IBGE de 7 dígitos.
    db_conn : duckdb.DuckDBPyConnection
        Deve conter: deficit_predito_h3_t0 (Etapa 4), proxy_setor (Etapa 1),
        mapeamento_h3_setor_t0 (Etapa 2-H3), pnadc_deficit_componentes (Grupo 5).
    tolerancia : float
        Reservado para compatibilidade futura (não usado na implementação atual).
    max_iter : int
        Reservado para compatibilidade futura (não usado na implementação atual).
    usar_ancora_pnadc : bool
        Se False, aplica apenas o Passo 1 (âncora setor). Útil quando o CV
        da PNADc é muito alto (>50%) e a âncora pode distorcer as estimativas.

    Retorna
    -------
    dict
        {"status": "ok"|"erro", "n_hexagonos": int, "n_calibrados": int,
         "deficit_predito_total": float, "deficit_calibrado_total": float,
         "total_pnadc": float, "fator_dominio": float,
         "cv_pnadc": float, "cv_alto": bool, "mensagem": str}
    """
    logger.info("[Etapa 5] Calibração t0 — municipio %s", codigo_ibge)

    # Verificar tabelas requeridas
    presentes = {r[0] for r in db_conn.execute("SHOW TABLES").fetchall()}
    requeridas = ["deficit_predito_h3_t0", "proxy_setor", "mapeamento_h3_setor_t0"]
    ausentes = [t for t in requeridas if t not in presentes]
    if ausentes:
        return {"status": "erro", "mensagem": f"Tabelas ausentes: {ausentes}"}

    # --- 1. Carregar predições H3 ---
    logger.info("[Etapa 5] Carregando deficit_predito_h3_t0")
    df_pred = db_conn.execute("""
        SELECT h3_index, h3_resolucao,
               proxy_predito, proxy_ic_lower, proxy_ic_upper,
               deficit_estimado, n_domicilios_grade, geometry
        FROM deficit_predito_h3_t0
    """).df()

    n_total = len(df_pred)
    n_sem_dom = int(df_pred["deficit_estimado"].isna().sum())
    if n_sem_dom > 0:
        logger.warning(
            "[Etapa 5] %d H3 sem deficit_estimado (n_domicilios_grade=NaN) — excluídos da calibração",
            n_sem_dom,
        )

    # Apenas H3 com domicílios participam da calibração
    df_valid = df_pred[df_pred["deficit_estimado"].notna()].copy()
    soma_pred = float(df_valid["deficit_estimado"].sum())

    # --- 2. Setor dominante por H3 ---
    logger.info("[Etapa 5] Atribuindo setor dominante a cada H3")
    map_df = db_conn.execute(
        "SELECT h3_index, cod_setor, peso_area FROM mapeamento_h3_setor_t0"
    ).df()

    setor_dom = (
        map_df.loc[map_df.groupby("h3_index")["peso_area"].idxmax(), ["h3_index", "cod_setor"]]
        .set_index("h3_index")["cod_setor"]
    )
    df_valid = df_valid.copy()
    df_valid["cod_setor"] = df_valid["h3_index"].map(setor_dom)

    n_sem_setor = int(df_valid["cod_setor"].isna().sum())
    if n_sem_setor > 0:
        logger.warning("[Etapa 5] %d H3 sem setor dominante — calibração setor ignorada para eles", n_sem_setor)

    # --- 3. Alvos por setor (Censo) ---
    logger.info("[Etapa 5] Calculando alvos por setor (proxy_carencias × n_dom)")
    proxy_s = db_conn.execute(
        "SELECT cod_setor, proxy_carencias_setor, n_dom_total FROM proxy_setor"
    ).df()
    proxy_s["proxy_total"] = proxy_s["proxy_carencias_setor"] * proxy_s["n_dom_total"]
    totais_setor = proxy_s.set_index("cod_setor")["proxy_total"]
    soma_alvo_setor = float(totais_setor.sum())

    logger.info(
        "[Etapa 5] Alvo Censo total=%.1f | Predito RF total=%.1f | razão=%.3f",
        soma_alvo_setor, soma_pred,
        soma_alvo_setor / soma_pred if soma_pred > 0 else np.nan,
    )

    # --- 4. Passo 1: distribuir alvo setor entre H3 (proporção do déficit predito) ---
    logger.info("[Etapa 5] Passo 1: distribuição intraurbana por setor (Censo)")
    df_cal = df_valid.copy()
    df_cal["deficit_cal1"] = np.nan
    df_cal["fator_calibracao_setor"] = np.nan

    mask_com_setor = df_cal["cod_setor"].notna()
    soma_por_setor = df_cal.loc[mask_com_setor].groupby("cod_setor")["deficit_estimado"].sum()

    for setor, soma_s in soma_por_setor.items():
        alvo_s = totais_setor.get(setor, np.nan)
        if np.isnan(alvo_s) or alvo_s <= 0:
            # Setor sem alvo válido: mantém proporção original
            fator = 1.0
        elif soma_s > 0:
            fator = alvo_s / soma_s
        else:
            fator = 1.0

        mask_s = mask_com_setor & (df_cal["cod_setor"] == setor)
        df_cal.loc[mask_s, "deficit_cal1"] = df_cal.loc[mask_s, "deficit_estimado"] * fator
        df_cal.loc[mask_s, "fator_calibracao_setor"] = fator

    # H3 sem setor: mantém valor predito original
    df_cal.loc[~mask_com_setor, "deficit_cal1"] = df_cal.loc[~mask_com_setor, "deficit_estimado"]
    df_cal.loc[~mask_com_setor, "fator_calibracao_setor"] = 1.0

    soma_cal1 = float(df_cal["deficit_cal1"].sum())
    logger.info("[Etapa 5] Passo 1 concluído: soma_cal1=%.1f", soma_cal1)

    # --- 5. Total PNADc (âncora macro) ---
    total_pnadc, cv_pnadc, cv_alto = _total_pnadc_sem_onus(db_conn)

    # --- 6. Passo 2: âncora global PNADc ---
    if usar_ancora_pnadc and not np.isnan(total_pnadc):
        if cv_alto:
            logger.warning(
                "[Etapa 5] CV PNADc = %.1f%% (alto). Âncora aplicada com ressalva. "
                "Considere usar_ancora_pnadc=False para preservar apenas a distribuição Censo.",
                cv_pnadc * 100,
            )

        fator_dominio = total_pnadc / soma_cal1 if soma_cal1 > 0 else 1.0
        df_cal["deficit_calibrado"] = df_cal["deficit_cal1"] * fator_dominio
        df_cal["fator_calibracao_dominio"] = fator_dominio
        logger.info(
            "[Etapa 5] Passo 2: fator_dominio=%.4f (PNADc=%.1f, cal1=%.1f)",
            fator_dominio, total_pnadc, soma_cal1,
        )
    else:
        if not usar_ancora_pnadc:
            logger.info("[Etapa 5] Âncora PNADc desabilitada — deficit_calibrado = Passo 1 apenas")
        else:
            logger.warning("[Etapa 5] pnadc_deficit_componentes vazia — usando apenas Passo 1")
        df_cal["deficit_calibrado"] = df_cal["deficit_cal1"]
        df_cal["fator_calibracao_dominio"] = 1.0
        fator_dominio = 1.0

    deficit_total = float(df_cal["deficit_calibrado"].sum())
    logger.info("[Etapa 5] Deficit calibrado total = %.1f", deficit_total)

    # --- 7. Montar tabela de saída (todos os 431 H3) ---
    resultado = df_pred[["h3_index", "h3_resolucao", "proxy_predito", "proxy_ic_lower",
                          "proxy_ic_upper", "n_domicilios_grade", "geometry"]].copy()
    resultado["deficit_predito"] = df_pred["deficit_estimado"]

    cal_idx = df_cal.set_index("h3_index")
    resultado["deficit_calibrado"] = resultado["h3_index"].map(cal_idx["deficit_calibrado"])
    resultado["fator_calibracao_setor"] = resultado["h3_index"].map(cal_idx["fator_calibracao_setor"])
    resultado["fator_calibracao_dominio"] = resultado["h3_index"].map(cal_idx["fator_calibracao_dominio"])
    resultado["cod_setor_dominante"] = resultado["h3_index"].map(setor_dom)

    # --- 8. Persistir no DuckDB ---
    db_conn.execute("DROP TABLE IF EXISTS deficit_calibrado_h3_t0")
    db_conn.execute("CREATE TABLE deficit_calibrado_h3_t0 AS SELECT * FROM resultado")
    logger.info("[Etapa 5] deficit_calibrado_h3_t0 salvo: %d linhas", len(resultado))

    # Metadados
    meta = pd.DataFrame([
        {"chave": "codigo_ibge", "valor": codigo_ibge},
        {"chave": "deficit_predito_total", "valor": str(round(soma_pred, 1))},
        {"chave": "deficit_cal1_total", "valor": str(round(soma_cal1, 1))},
        {"chave": "deficit_calibrado_total", "valor": str(round(deficit_total, 1))},
        {"chave": "alvo_censo_total", "valor": str(round(soma_alvo_setor, 1))},
        {"chave": "total_pnadc", "valor": str(round(total_pnadc, 1)) if not np.isnan(total_pnadc) else "NaN"},
        {"chave": "fator_dominio", "valor": str(round(fator_dominio, 4))},
        {"chave": "cv_pnadc", "valor": str(round(cv_pnadc, 4)) if not np.isnan(cv_pnadc) else "NaN"},
        {"chave": "cv_alto", "valor": str(cv_alto)},
        {"chave": "usar_ancora_pnadc", "valor": str(usar_ancora_pnadc)},
        {"chave": "n_hexagonos_total", "valor": str(n_total)},
        {"chave": "n_calibrados", "valor": str(int(resultado["deficit_calibrado"].notna().sum()))},
        {"chave": "n_sem_domicilios", "valor": str(n_sem_dom)},
    ])
    db_conn.execute("DROP TABLE IF EXISTS calibracao_metadados_t0")
    db_conn.execute("CREATE TABLE calibracao_metadados_t0 AS SELECT * FROM meta")
    logger.info("[Etapa 5] calibracao_metadados_t0 salvo")

    return {
        "status": "ok",
        "n_hexagonos": n_total,
        "n_calibrados": int(resultado["deficit_calibrado"].notna().sum()),
        "n_sem_domicilios": n_sem_dom,
        "deficit_predito_total": round(soma_pred, 1),
        "deficit_cal1_total": round(soma_cal1, 1),
        "deficit_calibrado_total": round(deficit_total, 1),
        "total_pnadc": round(total_pnadc, 1) if not np.isnan(total_pnadc) else None,
        "fator_dominio": round(fator_dominio, 4),
        "cv_pnadc": round(cv_pnadc, 4) if not np.isnan(cv_pnadc) else None,
        "cv_alto": cv_alto,
        "mensagem": (
            f"deficit_calibrado_h3_t0: {n_total} hexágonos, "
            f"deficit_calibrado={deficit_total:.0f} "
            f"(predito={soma_pred:.0f}, PNADc={'N/A' if np.isnan(total_pnadc) else f'{total_pnadc:.0f}'})."
        ),
    }
