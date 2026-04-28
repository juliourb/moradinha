"""
etapa7_modelo_temporal.py — Modelo temporal Δdéficit ~ Δcovariáveis no nível setor.

Motivação metodológica
───────────────────────
    Inspiração: BID (2019) "Estimating Housing Deficit in LAC". A metodologia BID
    usa regressão entre luminosidade noturna e déficit habitacional para gerar
    now-casts em nível de administração estadual/provincial (adm2).

    Adaptação do moradinha: regressão intraurbana entre setores dentro do mesmo
    município, com variação temporal como unidade de análise. Em vez de prever
    déficit absoluto (risco de extrapolação), prevemos a VARIAÇÃO do déficit
    entre t0 e t1 usando as variações das covariáveis anuais como preditores.

Modelo
──────
    Δproxy_total_setor = β₀ + β₁·Δlum_mean + β₂·Δprop_urbano +
                         β₃·flag_expansao + β₄·proxy_total_t0 + ε

    Onde:
        Δproxy_total_setor ≡ variação esperada do déficit bruto entre t0 e t1,
            expressa como proporção relativa ao t0 (evita escala absoluta).
        Δlum_mean          ≡ delta_lum_mean (Etapa 6)
        Δprop_urbano       ≡ delta_prop_urbano (Etapa 6)
        flag_expansao      ≡ 1 se delta_prop_urbano > 0.05
        proxy_total_t0     ≡ variável de controle para regressão à média

    Por que incluir proxy_total_t0 como preditor?
    ──────────────────────────────────────────────
    Regressão à média é um fenômeno estatístico em que unidades com valores
    extremos em t0 tendem a parecer mais próximas da média em t1, mesmo que
    NADA mude na realidade. Sem controle, o modelo confundiria essa tendência
    estatística com efeito causal de Δluminosidade.

    Incluindo proxy_total_t0 no modelo, isolamos a variação ACIMA DO ESPERADO
    pela regressão à média — que é a variação verdadeiramente explicada pelas
    mudanças nas covariáveis.

Abordagem de estimação em dois passos
──────────────────────────────────────
    Passo 1 — Predição proxy_t1:
        Aplica o modelo espacial da Etapa 3 (RF treinado em t0) às covariáveis
        de t1, substituindo apenas as variáveis anuais (lum, MapBiomas).
        → proxy_t1_predito por setor

    Passo 2 — Diagnóstico temporal (regressão de variações):
        Ajusta LM/RF: Δproxy_predito ~ Δlum + Δurbano + flag_expansao + proxy_t0
        Esse modelo é uma camada de interpretação e sanidade — os coeficientes
        devem ter sinal esperado (ex: flag_expansao > 0, Δlum > 0).
        → modelo_temporal_diagnostico + modelo_temporal.pkl

Modelos suportados
──────────────────
    'lm'  — Regressão linear (OLS).
            Limitação: a variação real de déficit em 2-3 anos é pequena →
            R² modesto esperado (0.2-0.4). Isso é normal e não invalida o método.

    'rf'  — Random Forest sobre as variações.
            Maior poder preditivo, mas menos interpretável.
            Recomendado se houver > 100 setores com variação não-nula.

Diagnósticos
─────────────
    - R² da regressão temporal (esperado: 0.2-0.4 em 2 anos, 0.4-0.6 em 5+ anos)
    - Sanidade: flag_expansao=1 → Δproxy > 0 na maioria dos casos
      (expansão urbana nova tende a gerar mais déficit qualitativo que quantitativo)
    - Setores com delta_prop_urbano negativo (desurbanização) devem ter Δproxy < 0

Pré-requisito
─────────────
    Etapa 5 (t0) completa — proxy_setor e deficit_calibrado_h3_t0 disponíveis.

Saída (tabelas no DuckDB)
─────────────────────────
    modelo_temporal_diagnostico:
        modelo (str), r2 (float), rmse (float), n_setores (int),
        coef_delta_lum (float), coef_delta_urbano (float),
        coef_flag_expansao (float), coef_proxy_t0 (float),
        timestamp (str)

    delta_proxy_setor_predito:
        cod_setor, delta_proxy_predito (float), delta_proxy_ic_lower (float),
        delta_proxy_ic_upper (float), geometry
"""

from __future__ import annotations

import json
import logging
import pickle
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

_MODELOS_SUPORTADOS = frozenset({"lm", "rf"})

# Features usadas no modelo espacial da Etapa 3 (mesmas 12)
_FEATURES_RF = [
    "renda_resp_media",
    "luminosidade_setor_mean",
    "luminosidade_setor_std",
    "cnefe_residencial_densidade",
    "cnefe_naoresid_densidade",
    "prop_urbano",
    "prop_mosaico_uso",
    "prop_vegetacao",
    "fcu_intersecta",
    "fcu_area_pct",
    "dist_centro_m",
    "cnefe_densidade_buffer_500m",
]

# Features do diagnóstico temporal
_FEATURES_DELTA = [
    "delta_lum_mean",
    "delta_prop_urbano",
    "flag_expansao",
    "proxy_carencias_setor",   # controle: regressão à média
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _verificar_tabelas(db_conn, tabelas: list[str]) -> list[str]:
    presentes = {r[0] for r in db_conn.execute("SHOW TABLES").fetchall()}
    return [t for t in tabelas if t not in presentes]


def _localizar_pkl_t0(codigo_ibge: str, output_dir: Path) -> Path:
    """
    Procura o pkl do modelo espacial t0 na pasta de output.

    Prefere RF (mais robusto para predição); cai para LM se RF não encontrado.
    """
    for modelo in ("rf", "lm"):
        candidato = output_dir / f"modelo_t0_{modelo}_{codigo_ibge}.pkl"
        if candidato.exists():
            logger.info("[Etapa 7] Modelo t0 encontrado: %s", candidato.name)
            return candidato
    raise FileNotFoundError(
        f"Modelo t0 não encontrado em '{output_dir}'. "
        f"Esperado: modelo_t0_rf_{codigo_ibge}.pkl ou modelo_t0_lm_{codigo_ibge}.pkl. "
        "Execute a Etapa 3 antes."
    )


def _construir_features_t1(db_conn) -> pd.DataFrame:
    """
    Monta matriz de features para predição em t1.

    Usa covariáveis congeladas de t0 e substitui luminosidade + MapBiomas pelas versões t1.
    Retorna DataFrame com cod_setor + 12 features (mesmos nomes do treino Etapa 3).
    """
    # Base: t0 completo (12 features + cod_setor)
    colunas_t0 = ", ".join(["cod_setor"] + _FEATURES_RF)
    df = db_conn.execute(f"SELECT {colunas_t0} FROM covariaveis_setor_t0").df()

    # t1: apenas as variáveis anualizáveis
    cov_t1 = db_conn.execute("""
        SELECT cod_setor,
               luminosidade_mean_t1,
               luminosidade_std_t1,
               prop_urbano_t1,
               prop_mosaico_uso_t1,
               prop_vegetacao_t1
        FROM covariaveis_setor_t1
    """).df()

    df = df.merge(cov_t1, on="cod_setor", how="left")

    # Sobrescreve com valores t1 onde disponíveis (NaN → mantém t0)
    df["luminosidade_setor_mean"] = df["luminosidade_mean_t1"].fillna(df["luminosidade_setor_mean"])
    df["luminosidade_setor_std"]  = df["luminosidade_std_t1"].fillna(df["luminosidade_setor_std"])
    df["prop_urbano"]              = df["prop_urbano_t1"].fillna(df["prop_urbano"])
    df["prop_mosaico_uso"]         = df["prop_mosaico_uso_t1"].fillna(df["prop_mosaico_uso"])
    df["prop_vegetacao"]           = df["prop_vegetacao_t1"].fillna(df["prop_vegetacao"])

    df["fcu_intersecta"] = df["fcu_intersecta"].fillna(False).astype(int)

    return df[["cod_setor"] + _FEATURES_RF]


def _predizer_com_rf(rf, X: pd.DataFrame) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Prediz proxy_t1 via RF. IC 90% via std entre árvores."""
    preds_arvores = np.column_stack([tree.predict(X) for tree in rf.estimators_])
    media  = preds_arvores.mean(axis=1)
    std    = preds_arvores.std(axis=1)
    lower  = media - 1.645 * std
    upper  = media + 1.645 * std
    return media, lower, upper


def _ajustar_diagnostico_lm(
    df_fit: pd.DataFrame,
    output_dir: Path,
    codigo_ibge: str,
) -> dict:
    import statsmodels.api as sm

    X = sm.add_constant(df_fit[_FEATURES_DELTA].astype(float))
    y = df_fit["delta_proxy_predito"]
    ols = sm.OLS(y, X).fit(cov_type="HC3")

    coefs = ols.params
    pkl_path = output_dir / f"modelo_temporal_lm_{codigo_ibge}.pkl"
    with open(pkl_path, "wb") as f:
        pickle.dump(ols, f)

    return {
        "r2": float(ols.rsquared),
        "rmse": float(np.sqrt((ols.resid ** 2).mean())),
        "coef_delta_lum": float(coefs.get("delta_lum_mean", np.nan)),
        "coef_delta_urbano": float(coefs.get("delta_prop_urbano", np.nan)),
        "coef_flag_expansao": float(coefs.get("flag_expansao", np.nan)),
        "coef_proxy_t0": float(coefs.get("proxy_carencias_setor", np.nan)),
        "pkl_path": str(pkl_path),
    }


def _ajustar_diagnostico_rf(
    df_fit: pd.DataFrame,
    output_dir: Path,
    codigo_ibge: str,
) -> dict:
    from sklearn.ensemble import RandomForestRegressor

    X = df_fit[_FEATURES_DELTA].astype(float)
    y = df_fit["delta_proxy_predito"]

    rf_delta = RandomForestRegressor(n_estimators=200, max_features="sqrt", random_state=42, n_jobs=-1)
    rf_delta.fit(X, y)

    y_pred = rf_delta.predict(X)
    r2   = float(rf_delta.score(X, y))
    rmse = float(np.sqrt(((y - y_pred) ** 2).mean()))

    pkl_path = output_dir / f"modelo_temporal_rf_{codigo_ibge}.pkl"
    with open(pkl_path, "wb") as f:
        pickle.dump(rf_delta, f)

    # Feature importances como proxy de coeficientes
    imp = dict(zip(X.columns, rf_delta.feature_importances_))

    return {
        "r2": r2,
        "rmse": rmse,
        "coef_delta_lum": float(imp.get("delta_lum_mean", np.nan)),
        "coef_delta_urbano": float(imp.get("delta_prop_urbano", np.nan)),
        "coef_flag_expansao": float(imp.get("flag_expansao", np.nan)),
        "coef_proxy_t0": float(imp.get("proxy_carencias_setor", np.nan)),
        "pkl_path": str(pkl_path),
    }


# ---------------------------------------------------------------------------
# Função pública
# ---------------------------------------------------------------------------

def ajustar_modelo_temporal(
    codigo_ibge: str,
    ano_t0: int,
    ano_t1: int,
    db_conn,
    modelo: str = "lm",
    output_dir: Path | None = None,
) -> dict:
    """
    Ajusta o modelo temporal Δproxy ~ Δcovariáveis no nível setor.

    Parâmetros
    ----------
    codigo_ibge : str
        Código IBGE de 7 dígitos.
    ano_t0 : int
        Ano-base (ex: 2022).
    ano_t1 : int
        Ano-corrente (ex: 2024).
    db_conn : duckdb.DuckDBPyConnection
        Deve conter: proxy_setor (Etapa 1), delta_covariaveis_setor (Etapa 6),
        deficit_calibrado_h3_t0 (Etapa 5).
    modelo : str
        'lm' (padrão, mais interpretável) ou 'rf'.
    output_dir : Path | None
        Pasta para serializar modelo_temporal.pkl.

    Retorna
    -------
    dict
        {"status": "ok"|"erro", "r2": float, "rmse": float, "coeficientes": dict}
    """
    if modelo not in _MODELOS_SUPORTADOS:
        return {"status": "erro", "mensagem": f"modelo '{modelo}' não suportado. Use: {_MODELOS_SUPORTADOS}"}

    logger.info(
        "[Etapa 7] Ajustando modelo temporal '%s' — municipio %s, t0=%d, t1=%d",
        modelo, codigo_ibge, ano_t0, ano_t1,
    )

    # --- Verificar dependências ---
    ausentes = _verificar_tabelas(db_conn, [
        "proxy_setor", "covariaveis_setor_t0",
        "covariaveis_setor_t1", "delta_covariaveis_setor",
    ])
    if ausentes:
        msg = f"Tabelas ausentes: {ausentes}"
        logger.error("[Etapa 7] %s", msg)
        return {"status": "erro", "mensagem": msg}

    # --- Output dir ---
    if output_dir is None:
        output_dir = Path(f"data/processed/al_{codigo_ibge}")
        # tenta pasta nomeada pelo município (convenção do orquestrador)
        diag = db_conn.execute(
            "SELECT * FROM modelo_t0_diagnostico LIMIT 1"
        ).df() if "modelo_t0_diagnostico" in {r[0] for r in db_conn.execute("SHOW TABLES").fetchall()} else None

    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # --- Passo 1: localizar e carregar modelo t0 (RF preferido) ---
    try:
        pkl_t0 = _localizar_pkl_t0(codigo_ibge, output_dir)
    except FileNotFoundError as e:
        return {"status": "erro", "mensagem": str(e)}

    with open(pkl_t0, "rb") as f:
        modelo_t0 = pickle.load(f)

    tipo_modelo_t0 = "rf" if hasattr(modelo_t0, "estimators_") else "lm"
    logger.info("[Etapa 7] Tipo do modelo t0: %s", tipo_modelo_t0)

    # --- Passo 2: construir matriz de features t1 ---
    logger.info("[Etapa 7] Construindo matriz de features t1")
    df_feat = _construir_features_t1(db_conn)

    X_t1 = df_feat[_FEATURES_RF].copy()
    # Imputação por mediana (igual Etapa 4)
    medians = X_t1.median()
    n_imputed = X_t1.isna().sum().sum()
    X_t1 = X_t1.fillna(medians)
    logger.info("[Etapa 7] Features t1: %d setores | %d valores imputados", len(X_t1), n_imputed)

    # --- Passo 3: predizer proxy_t1 e IC ---
    logger.info("[Etapa 7] Predizendo proxy_t1 com modelo t0")
    if tipo_modelo_t0 == "rf":
        proxy_t1_pred, ic_lower, ic_upper = _predizer_com_rf(modelo_t0, X_t1)
    else:
        import statsmodels.api as sm
        X_const = sm.add_constant(X_t1, has_constant="add")
        proxy_t1_pred = modelo_t0.predict(X_const).values
        # IC via intervalo de predição OLS (90%)
        pred_obj = modelo_t0.get_prediction(X_const)
        ic_frame = pred_obj.summary_frame(alpha=0.10)
        ic_lower = ic_frame["obs_ci_lower"].values
        ic_upper = ic_frame["obs_ci_upper"].values

    # --- Passo 4: carregar proxy_t0 e calcular delta ---
    proxy_t0_df = db_conn.execute(
        "SELECT cod_setor, proxy_carencias_setor FROM proxy_setor"
    ).df()

    df_feat["proxy_t1_predito"] = proxy_t1_pred
    df_feat["ic_lower_t1"] = ic_lower
    df_feat["ic_upper_t1"] = ic_upper
    df_feat = df_feat.merge(proxy_t0_df, on="cod_setor", how="left")

    df_feat["delta_proxy_predito"] = df_feat["proxy_t1_predito"] - df_feat["proxy_carencias_setor"]
    df_feat["delta_proxy_ic_lower"] = df_feat["ic_lower_t1"] - df_feat["proxy_carencias_setor"]
    df_feat["delta_proxy_ic_upper"] = df_feat["ic_upper_t1"] - df_feat["proxy_carencias_setor"]

    logger.info(
        "[Etapa 7] delta_proxy — media=%.4f | min=%.4f | max=%.4f",
        df_feat["delta_proxy_predito"].mean(),
        df_feat["delta_proxy_predito"].min(),
        df_feat["delta_proxy_predito"].max(),
    )

    # --- Passo 5: diagnóstico temporal (LM/RF sobre variações) ---
    delta_cov = db_conn.execute("""
        SELECT cod_setor, delta_lum_mean, delta_prop_urbano, flag_expansao
        FROM delta_covariaveis_setor
    """).df()

    df_fit = df_feat.merge(delta_cov, on="cod_setor", how="left")
    df_fit["flag_expansao"] = df_fit["flag_expansao"].fillna(False).astype(int)
    df_fit = df_fit.dropna(subset=["delta_proxy_predito"] + _FEATURES_DELTA)
    n_fit = len(df_fit)
    logger.info("[Etapa 7] Dataset diagnóstico: %d setores (com todos os campos)", n_fit)

    if n_fit < 10:
        logger.warning("[Etapa 7] Poucos setores para diagnóstico (%d) — pulando regressão.", n_fit)
        diag_res = {"r2": np.nan, "rmse": np.nan,
                    "coef_delta_lum": np.nan, "coef_delta_urbano": np.nan,
                    "coef_flag_expansao": np.nan, "coef_proxy_t0": np.nan,
                    "pkl_path": ""}
    else:
        try:
            if modelo == "lm":
                diag_res = _ajustar_diagnostico_lm(df_fit, output_dir, codigo_ibge)
            else:
                diag_res = _ajustar_diagnostico_rf(df_fit, output_dir, codigo_ibge)
            logger.info(
                "[Etapa 7] Diagnóstico %s: R²=%.3f | RMSE=%.4f",
                modelo, diag_res["r2"], diag_res["rmse"],
            )
        except Exception as e:
            logger.error("[Etapa 7] Diagnóstico falhou: %s", e, exc_info=True)
            return {"status": "erro", "mensagem": f"Diagnóstico temporal falhou: {e}"}

    # Sanidade: flag_expansao=1 deve ter Δproxy > 0 na maioria dos casos
    n_expansao_positivo = int(
        df_feat[df_feat["delta_proxy_predito"] > 0].shape[0]
    )
    logger.info(
        "[Etapa 7] Sanidade — setores com Δproxy > 0: %d/%d",
        n_expansao_positivo, len(df_feat),
    )

    # --- Passo 6: salvar tabelas ---
    geo = db_conn.execute(
        "SELECT cod_setor, geometry FROM covariaveis_setor_t0"
    ).df()

    out = df_feat[[
        "cod_setor",
        "delta_proxy_predito",
        "delta_proxy_ic_lower",
        "delta_proxy_ic_upper",
    ]].merge(geo, on="cod_setor", how="left")

    db_conn.execute("DROP TABLE IF EXISTS delta_proxy_setor_predito")
    db_conn.execute("CREATE TABLE delta_proxy_setor_predito AS SELECT * FROM out")
    logger.info("[Etapa 7] delta_proxy_setor_predito salvo: %d linhas", len(out))

    diag_df = pd.DataFrame([{
        "modelo": modelo,
        "r2": diag_res["r2"],
        "rmse": diag_res["rmse"],
        "n_setores": n_fit,
        "coef_delta_lum": diag_res["coef_delta_lum"],
        "coef_delta_urbano": diag_res["coef_delta_urbano"],
        "coef_flag_expansao": diag_res["coef_flag_expansao"],
        "coef_proxy_t0": diag_res["coef_proxy_t0"],
        "n_imputed_features": int(n_imputed),
        "ano_t0": ano_t0,
        "ano_t1": ano_t1,
        "pkl_t0_usado": str(pkl_t0),
        "pkl_diagnostico": diag_res.get("pkl_path", ""),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }])

    db_conn.execute("DROP TABLE IF EXISTS modelo_temporal_diagnostico")
    db_conn.execute("CREATE TABLE modelo_temporal_diagnostico AS SELECT * FROM diag_df")
    logger.info("[Etapa 7] modelo_temporal_diagnostico salvo")

    coefs = {
        "delta_lum_mean": diag_res["coef_delta_lum"],
        "delta_prop_urbano": diag_res["coef_delta_urbano"],
        "flag_expansao": diag_res["coef_flag_expansao"],
        "proxy_carencias_setor": diag_res["coef_proxy_t0"],
    }

    return {
        "status": "ok",
        "camadas": ["delta_proxy_setor_predito", "modelo_temporal_diagnostico"],
        "r2": diag_res["r2"],
        "rmse": diag_res["rmse"],
        "n_setores": n_fit,
        "coeficientes": coefs,
        "n_delta_positivo": n_expansao_positivo,
    }
