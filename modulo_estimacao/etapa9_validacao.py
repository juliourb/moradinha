"""
etapa9_validacao.py — Validação transversal das estimativas t0 e t1.

Módulos implementados
─────────────────────
    A. CV 5-fold intra-município (setores)
       Treina o modelo espacial em 80% dos setores, prediz os 20% restantes,
       compara com proxy_setor observado. Proxy de validação cruzada do modelo.

    B. Moran's I nos resíduos (lido de modelo_t0_diagnostico — já calculado na E3)

    C. Consistência de escala
       t0: |Σ deficit_calibrado_h3 por setor - proxy_setor × n_dom| < tolerância
       t1: Δ total t1 vs t0 (sem âncora PNADc t1, verificação interna)

    D. Comparação com FJP publicado (opcional)
       Lê data/raw/referencias/fjp_municipios.json se disponível.
       Compara soma H3 com total FJP do município. Diferença esperada: 10-20%.

    E. Sanity checks
       - Setores com FCU devem ter deficit_calibrado acima da média
       - Setores com flag_expansao=True devem ter deficit_t1 > deficit_t0
       - Luminosidade e déficit devem ter correlação negativa
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _verificar_tabelas(db_conn, tabelas: list[str]) -> list[str]:
    presentes = {r[0] for r in db_conn.execute("SHOW TABLES").fetchall()}
    return [t for t in tabelas if t not in presentes]


def _detectar_periodos(db_conn) -> list[str]:
    presentes = {r[0] for r in db_conn.execute("SHOW TABLES").fetchall()}
    periodos = []
    if "deficit_calibrado_h3_t0" in presentes:
        periodos.append("t0")
    if "deficit_calibrado_h3_t1" in presentes:
        periodos.append("t1")
    return periodos


# ---------------------------------------------------------------------------
# Módulo A — CV 5-fold no nível setor
# ---------------------------------------------------------------------------

def _modulo_a_cv_setor(db_conn) -> dict:
    """
    CV 5-fold intra-município: treina RF em 80% dos setores, prediz 20%.

    Usa as mesmas features e target da Etapa 3.
    """
    from sklearn.ensemble import RandomForestRegressor
    from sklearn.model_selection import KFold

    _FEATURES = [
        "renda_resp_media", "luminosidade_setor_mean", "luminosidade_setor_std",
        "cnefe_residencial_densidade", "cnefe_naoresid_densidade",
        "prop_urbano", "prop_mosaico_uso", "prop_vegetacao",
        "fcu_intersecta", "fcu_area_pct", "dist_centro_m", "cnefe_densidade_buffer_500m",
    ]

    try:
        proxy = db_conn.execute(
            "SELECT cod_setor, proxy_carencias_setor FROM proxy_setor"
        ).df()
        cov = db_conn.execute(
            f"SELECT cod_setor, {', '.join(_FEATURES)} FROM covariaveis_setor_t0"
        ).df()
        df = proxy.merge(cov, on="cod_setor", how="inner")
        df["fcu_intersecta"] = df["fcu_intersecta"].fillna(False).astype(int)
        df = df.dropna(subset=["proxy_carencias_setor"] + _FEATURES).reset_index(drop=True)

        if len(df) < 20:
            return {"r2_cv": np.nan, "rmse_cv": np.nan, "n_setores": len(df), "erro": "poucos setores"}

        X = df[_FEATURES].values
        y = df["proxy_carencias_setor"].values

        kf = KFold(n_splits=5, shuffle=True, random_state=42)
        preds = np.full(len(y), np.nan)

        for train_idx, val_idx in kf.split(X):
            rf = RandomForestRegressor(
                n_estimators=200, max_features="sqrt", min_samples_leaf=3,
                random_state=42, n_jobs=-1,
            )
            rf.fit(X[train_idx], y[train_idx])
            preds[val_idx] = rf.predict(X[val_idx])

        resid = y - preds
        ss_res = float(np.sum(resid ** 2))
        ss_tot = float(np.sum((y - y.mean()) ** 2))
        r2_cv  = float(1 - ss_res / ss_tot) if ss_tot > 0 else np.nan
        rmse_cv = float(np.sqrt(np.mean(resid ** 2)))
        bias    = float(np.mean(resid))

        logger.info(
            "[Etapa 9 / Módulo A] CV 5-fold: R²=%.3f | RMSE=%.4f | bias=%.4f | n=%d",
            r2_cv, rmse_cv, bias, len(df),
        )
        return {"r2_cv": r2_cv, "rmse_cv": rmse_cv, "bias_cv": bias, "n_setores": len(df)}

    except Exception as e:
        logger.warning("[Etapa 9 / Módulo A] Falhou: %s", e)
        return {"r2_cv": np.nan, "rmse_cv": np.nan, "n_setores": 0, "erro": str(e)}


# ---------------------------------------------------------------------------
# Módulo B — Moran's I (lido de modelo_t0_diagnostico)
# ---------------------------------------------------------------------------

def _modulo_b_moran(db_conn) -> tuple[float, float]:
    try:
        row = db_conn.execute(
            "SELECT moran_i_residuos, moran_p FROM modelo_t0_diagnostico LIMIT 1"
        ).fetchone()
        if row:
            mi, mp = float(row[0]) if row[0] is not None else np.nan, \
                     float(row[1]) if row[1] is not None else np.nan
            sinal = "OK" if (np.isnan(mi) or mi < 0.10) else "ALERTA"
            logger.info(
                "[Etapa 9 / Módulo B] Moran's I = %.4f (p=%.3f) — %s",
                mi if not np.isnan(mi) else -99,
                mp if not np.isnan(mp) else -99,
                sinal,
            )
            return mi, mp
    except Exception as e:
        logger.warning("[Etapa 9 / Módulo B] Falhou: %s", e)
    return np.nan, np.nan


# ---------------------------------------------------------------------------
# Módulo C — Consistência de escala
# ---------------------------------------------------------------------------

def _modulo_c_consistencia(db_conn, periodos: list[str]) -> dict:
    resultados = {}

    if "t0" in periodos:
        try:
            # Compara soma H3 calibrada com âncora PNADc (calibração em 2 passos usa PNADc como alvo)
            soma_h3 = db_conn.execute(
                "SELECT SUM(deficit_calibrado) FROM deficit_calibrado_h3_t0"
            ).fetchone()[0]
            soma_h3 = float(soma_h3) if soma_h3 else 0.0

            pnadc_total = None
            try:
                pnadc_total = db_conn.execute("""
                    SELECT SUM(total_estimado)
                    FROM pnadc_deficit_componentes
                    WHERE componente IN ('habitacao_precaria', 'coabitacao')
                """).fetchone()[0]
                pnadc_total = float(pnadc_total) if pnadc_total else None
            except Exception:
                pass

            if pnadc_total and pnadc_total > 0:
                delta_pnadc = abs(soma_h3 - pnadc_total) / pnadc_total
                consistente = delta_pnadc < 0.05
                logger.info(
                    "[Etapa 9 / Módulo C] t0: soma_h3=%.1f | pnadc=%.1f | delta=%.2f%% — %s",
                    soma_h3, pnadc_total, delta_pnadc * 100,
                    "OK" if consistente else "DIVERGÊNCIA",
                )
                resultados["t0"] = {
                    "soma_h3_total": round(soma_h3, 1),
                    "pnadc_total": round(pnadc_total, 1),
                    "delta_pct": round(delta_pnadc * 100, 2),
                    "consistente": consistente,
                }
            else:
                logger.info(
                    "[Etapa 9 / Módulo C] t0: soma_h3=%.1f | PNADc indisponível para comparação",
                    soma_h3,
                )
                resultados["t0"] = {
                    "soma_h3_total": round(soma_h3, 1),
                    "pnadc_total": None,
                    "consistente": None,
                }
        except Exception as e:
            logger.warning("[Etapa 9 / Módulo C] t0 falhou: %s", e)
            resultados["t0"] = {"erro": str(e)}

    if "t1" in periodos:
        try:
            totais = db_conn.execute("""
                SELECT
                    SUM(t0.deficit_calibrado) AS total_t0,
                    SUM(t1.deficit_calibrado_t1) AS total_t1,
                    SUM(t1.deficit_calibrado_t1) - SUM(t0.deficit_calibrado) AS delta_abs,
                    (SUM(t1.deficit_calibrado_t1) - SUM(t0.deficit_calibrado))
                        / NULLIF(SUM(t0.deficit_calibrado), 0) AS delta_rel
                FROM deficit_calibrado_h3_t0 t0
                JOIN deficit_calibrado_h3_t1 t1 ON t0.h3_index = t1.h3_index
            """).fetchone()
            delta_rel_t1 = float(totais[3]) if totais[3] is not None else np.nan
            logger.info(
                "[Etapa 9 / Módulo C] t1: total_t0=%.1f | total_t1=%.1f | delta=%.2f%%",
                totais[0], totais[1], delta_rel_t1 * 100,
            )
            resultados["t1"] = {
                "total_t0": round(float(totais[0]), 1),
                "total_t1": round(float(totais[1]), 1),
                "delta_abs": round(float(totais[2]), 1),
                "delta_rel_pct": round(delta_rel_t1 * 100, 2),
            }
        except Exception as e:
            logger.warning("[Etapa 9 / Módulo C] t1 falhou: %s", e)
            resultados["t1"] = {"erro": str(e)}

    return resultados


# ---------------------------------------------------------------------------
# Módulo D — Comparação FJP
# ---------------------------------------------------------------------------

def _modulo_d_fjp(codigo_ibge: str, db_conn) -> dict:
    fjp_path = Path("data/raw/referencias/fjp_municipios.json")
    if not fjp_path.exists():
        logger.info("[Etapa 9 / Módulo D] %s não encontrado — comparação FJP pulada.", fjp_path)
        return {"disponivel": False}

    try:
        with open(fjp_path, encoding="utf-8") as f:
            fjp_data = json.load(f)

        fjp_total = fjp_data.get(codigo_ibge, {}).get("deficit_total")
        if fjp_total is None:
            logger.info("[Etapa 9 / Módulo D] Município %s não encontrado no arquivo FJP.", codigo_ibge)
            return {"disponivel": False}

        nosso_total = db_conn.execute(
            "SELECT SUM(deficit_calibrado) FROM deficit_calibrado_h3_t0"
        ).fetchone()[0]
        nosso_total = float(nosso_total) if nosso_total else np.nan

        delta_pct = (nosso_total - fjp_total) / fjp_total * 100 if fjp_total else np.nan
        logger.info(
            "[Etapa 9 / Módulo D] FJP=%s | Nosso=%.1f | delta=%.1f%%",
            fjp_total, nosso_total, delta_pct,
        )
        return {
            "disponivel": True,
            "fjp_total": fjp_total,
            "nosso_total": round(nosso_total, 1),
            "delta_pct": round(delta_pct, 1),
            "dentro_faixa": abs(delta_pct) <= 30,
        }
    except Exception as e:
        logger.warning("[Etapa 9 / Módulo D] Falhou: %s", e)
        return {"disponivel": False, "erro": str(e)}


# ---------------------------------------------------------------------------
# Módulo E — Sanity checks
# ---------------------------------------------------------------------------

def _modulo_e_sanidade(db_conn, periodos: list[str]) -> dict:
    resultados = {}
    alertas = []

    # E1: Setores com FCU devem ter taxa de carência (proxy_setor) maior que setores sem FCU
    try:
        fcu_sanity = db_conn.execute("""
            SELECT
                fcu.fcu_intersecta,
                AVG(p.proxy_carencias_setor) AS media_proxy,
                COUNT(*) AS n
            FROM fcu_setor fcu
            JOIN proxy_setor p ON p.cod_setor = LEFT(fcu.cod_setor, 15)
            GROUP BY fcu.fcu_intersecta
        """).df()

        if len(fcu_sanity) >= 2:
            med_fcu  = float(fcu_sanity.loc[fcu_sanity["fcu_intersecta"] == True,  "media_proxy"].values[0]) if True  in fcu_sanity["fcu_intersecta"].values else np.nan
            med_nfcu = float(fcu_sanity.loc[fcu_sanity["fcu_intersecta"] == False, "media_proxy"].values[0]) if False in fcu_sanity["fcu_intersecta"].values else np.nan
            fcu_ok = med_fcu > med_nfcu if not (np.isnan(med_fcu) or np.isnan(med_nfcu)) else None
            logger.info(
                "[Etapa 9 / Módulo E] FCU: proxy_fcu=%.3f | proxy_nfcu=%.3f | fcu>nfcu=%s",
                med_fcu if not np.isnan(med_fcu) else -99,
                med_nfcu if not np.isnan(med_nfcu) else -99,
                fcu_ok,
            )
            resultados["fcu_deficit_maior"] = fcu_ok
            if fcu_ok is False:
                alertas.append("Taxa de carência FCU é menor que não-FCU — verificar dados FCU.")
        else:
            logger.info("[Etapa 9 / Módulo E] FCU: menos de 2 grupos — check pulado.")
            resultados["fcu_deficit_maior"] = None

    except Exception as e:
        logger.warning("[Etapa 9 / Módulo E] FCU check falhou: %s", e)

    # E2: flag_expansao → deficit_t1 > deficit_t0
    if "t1" in periodos:
        try:
            exp_check = db_conn.execute("""
                SELECT
                    d.flag_expansao,
                    AVG(t1.deficit_calibrado_t1 - t0.deficit_calibrado) AS delta_medio,
                    COUNT(*) AS n
                FROM delta_covariaveis_setor d
                JOIN deficit_calibrado_h3_t0 t0 ON t0.cod_setor_dominante = d.cod_setor
                JOIN deficit_calibrado_h3_t1 t1 ON t1.h3_index = t0.h3_index
                GROUP BY d.flag_expansao
            """).df()

            if len(exp_check) >= 2:
                delta_expansao  = float(exp_check.loc[exp_check["flag_expansao"] == True,  "delta_medio"].values[0]) if True  in exp_check["flag_expansao"].values else np.nan
                delta_nexpansao = float(exp_check.loc[exp_check["flag_expansao"] == False, "delta_medio"].values[0]) if False in exp_check["flag_expansao"].values else np.nan
                exp_ok = delta_expansao > delta_nexpansao if not (np.isnan(delta_expansao) or np.isnan(delta_nexpansao)) else None
                logger.info(
                    "[Etapa 9 / Módulo E] Expansão: delta_expansao=%.3f | delta_normal=%.3f | exp>normal=%s",
                    delta_expansao, delta_nexpansao, exp_ok,
                )
                resultados["expansao_delta_maior"] = exp_ok
                if exp_ok is False:
                    alertas.append("Setores com flag_expansao têm variação de déficit MENOR — verificar modelo temporal.")
        except Exception as e:
            logger.warning("[Etapa 9 / Módulo E] Expansão check falhou: %s", e)

    # E3: Correlação luminosidade × taxa de carência (esperada negativa)
    # Usa proxy_carencias_setor (taxa) para evitar viés de tamanho de setor
    try:
        corr = db_conn.execute("""
            SELECT CORR(c.luminosidade_setor_mean, p.proxy_carencias_setor) AS corr_lum_proxy
            FROM covariaveis_setor_t0 c
            JOIN proxy_setor p ON p.cod_setor = c.cod_setor
        """).fetchone()[0]
        corr = float(corr) if corr is not None else np.nan
        logger.info("[Etapa 9 / Módulo E] Correlação lum × proxy_carencias = %.3f (esperado < 0)", corr)
        resultados["corr_lum_deficit"] = round(corr, 3)
        if not np.isnan(corr) and corr > 0.1:
            alertas.append(f"Correlação lum×carência positiva ({corr:.2f}) — inesperado.")
    except Exception as e:
        logger.warning("[Etapa 9 / Módulo E] Correlação falhou: %s", e)

    resultados["alertas"] = alertas
    return resultados


# ---------------------------------------------------------------------------
# Função pública
# ---------------------------------------------------------------------------

def validar_estimativas(
    codigo_ibge: str,
    db_conn,
    periodos: list[str] | None = None,
    comparar_fjp: bool = True,
) -> dict:
    """
    Executa todos os módulos de validação para os períodos especificados.

    Parâmetros
    ----------
    codigo_ibge : str
        Código IBGE de 7 dígitos.
    db_conn : duckdb.DuckDBPyConnection
        Deve conter as tabelas da Etapa 5 (t0) e/ou Etapa 8 (t1).
    periodos : list[str] | None
        ['t0'], ['t1'] ou ['t0', 't1']. Se None, valida todos disponíveis.
    comparar_fjp : bool
        Se True, tenta comparar com FJP publicado (requer arquivo local).

    Retorna
    -------
    dict
        {"status": "ok"|"erro", "camadas": [...], "alertas": [...], "resumo": {...}}
    """
    logger.info("[Etapa 9] Validação transversal — municipio %s", codigo_ibge)

    periodos = periodos or _detectar_periodos(db_conn)
    if not periodos:
        return {"status": "erro", "mensagem": "Nenhum período disponível para validação."}

    logger.info("[Etapa 9] Períodos detectados: %s", periodos)
    alertas = []
    camadas = []

    # --- Módulo A: CV 5-fold ---
    ausentes_a = _verificar_tabelas(db_conn, ["proxy_setor", "covariaveis_setor_t0"])
    if ausentes_a:
        logger.warning("[Etapa 9 / Módulo A] Tabelas ausentes %s — pulado", ausentes_a)
        res_a = {"r2_cv": np.nan, "rmse_cv": np.nan}
    else:
        logger.info("[Etapa 9] Módulo A — CV 5-fold setor")
        res_a = _modulo_a_cv_setor(db_conn)

    # --- Módulo B: Moran's I ---
    logger.info("[Etapa 9] Módulo B — Moran's I")
    moran_i, moran_p = _modulo_b_moran(db_conn)
    if not np.isnan(moran_i) and moran_i >= 0.10 and not np.isnan(moran_p) and moran_p < 0.05:
        alertas.append(f"Moran's I = {moran_i:.3f} (p={moran_p:.3f}) — resíduos agrupados. Considere GWR.")

    # --- Módulo C: Consistência ---
    logger.info("[Etapa 9] Módulo C — Consistência de escala")
    res_c = _modulo_c_consistencia(db_conn, periodos)
    if "t0" in res_c and res_c["t0"].get("consistente") is False:
        alertas.append(
            f"Consistência t0: soma H3 diverge de PNADc em {res_c['t0'].get('delta_pct', '?')}%."
        )

    # --- Módulo D: FJP ---
    if comparar_fjp:
        logger.info("[Etapa 9] Módulo D — Comparação FJP")
        res_d = _modulo_d_fjp(codigo_ibge, db_conn)
        if res_d.get("disponivel") and not res_d.get("dentro_faixa", True):
            alertas.append(f"Diferença FJP: {res_d['delta_pct']:.1f}% (fora da faixa ±30%).")
    else:
        res_d = {"disponivel": False}

    # --- Módulo E: Sanity checks ---
    logger.info("[Etapa 9] Módulo E — Sanity checks")
    res_e = _modulo_e_sanidade(db_conn, periodos)
    alertas.extend(res_e.pop("alertas", []))

    # --- Salvar resumo ---
    resumo = {
        "r2_cv_5fold": res_a.get("r2_cv"),
        "rmse_cv_5fold": res_a.get("rmse_cv"),
        "n_setores_cv": res_a.get("n_setores"),
        "moran_i": moran_i if not np.isnan(moran_i) else None,
        "moran_p": moran_p if not np.isnan(moran_p) else None,
        "consistencia_t0_pct_ok": res_c.get("t0", {}).get("pct_ok"),
        "delta_t0_t1_pct": res_c.get("t1", {}).get("delta_rel_pct"),
        "fjp_disponivel": res_d.get("disponivel", False),
        "fjp_delta_pct": res_d.get("delta_pct"),
        "corr_lum_deficit": res_e.get("corr_lum_deficit"),
        "fcu_deficit_maior": res_e.get("fcu_deficit_maior"),
        "expansao_delta_maior": res_e.get("expansao_delta_maior"),
        "n_alertas": len(alertas),
    }

    resumo_df = pd.DataFrame([{
        "periodo": "/".join(periodos),
        **{k: (str(v) if v is not None else None) for k, v in resumo.items()},
        "alertas": " | ".join(alertas) if alertas else "",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }])
    db_conn.execute("DROP TABLE IF EXISTS validacao_resumo")
    db_conn.execute("CREATE TABLE validacao_resumo AS SELECT * FROM resumo_df")
    camadas.append("validacao_resumo")
    logger.info("[Etapa 9] validacao_resumo salvo | alertas: %d", len(alertas))

    if alertas:
        for a in alertas:
            logger.warning("[Etapa 9] ALERTA: %s", a)

    return {
        "status": "ok",
        "camadas": camadas,
        "alertas": alertas,
        "resumo": resumo,
        "modulo_a": res_a,
        "modulo_b": {"moran_i": moran_i, "moran_p": moran_p},
        "modulo_c": res_c,
        "modulo_d": res_d,
        "modulo_e": res_e,
    }
