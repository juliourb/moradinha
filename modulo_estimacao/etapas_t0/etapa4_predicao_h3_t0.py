"""
etapa4_predicao_h3_t0.py — Projeção das predições do modelo para a grade H3 (t0).

Aplica o modelo da Etapa 3 (treinado em setores) sobre as covariáveis H3 (Etapa 2)
para gerar estimativas de carências habitacionais por hexágono.

Saída — tabela `deficit_predito_h3_t0` no DuckDB:
    h3_index           str    — índice H3 (ex: '8881ae4019fffff')
    h3_resolucao       int    — resolução H3
    proxy_predito      float  — proxy_carencias_setor predito (0–1)
    proxy_ic_lower     float  — IC inferior 90% (aproximado)
    proxy_ic_upper     float  — IC superior 90% (aproximado)
    deficit_estimado   float  — proxy_predito * n_domicilios_grade (contagem bruta)
    n_domicilios_grade int    — domicílios IBGE 200m agregados no H3
    n_imputed          int    — número de features imputadas por mediana (0 = completo)
    geometry           BLOB   — WKB do polígono H3

Notas sobre incerteza:
    Para RF: IC aproximado via desvio-padrão entre árvores (std dos estimators).
    Não é um IC formal, mas é um indicador de dispersão de predição robusto.
    Para LM: IC formal via erro padrão da predição (statsmodels OLS.get_prediction).
"""

from __future__ import annotations

import logging
import pickle
from pathlib import Path

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

_FEATURES = [
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


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _detectar_modelo(db_conn) -> str:
    """Lê tipo de modelo da tabela modelo_t0_diagnostico."""
    try:
        row = db_conn.execute("SELECT modelo FROM modelo_t0_diagnostico LIMIT 1").fetchone()
        return row[0] if row else "rf"
    except Exception:
        return "rf"


def _carregar_modelo(output_dir: Path, codigo_ibge: str, tipo: str):
    """Carrega modelo serializado em .pkl."""
    pkl_path = output_dir / f"modelo_t0_{tipo}_{codigo_ibge}.pkl"
    if not pkl_path.exists():
        raise FileNotFoundError(f"Modelo nao encontrado: {pkl_path}")
    with open(pkl_path, "rb") as f:
        return pickle.load(f)


def _imputar_mediana(df: pd.DataFrame, features: list[str]) -> tuple[pd.DataFrame, pd.Series]:
    """
    Imputa NaN com mediana por coluna.

    Retorna (df_imputado, n_imputed por linha).
    """
    df_imp = df.copy()
    n_nan_por_linha = df[features].isna().sum(axis=1)
    for col in features:
        med = df[col].median()
        df_imp[col] = df[col].fillna(med)
    return df_imp, n_nan_por_linha


def _h3_cell_polygon_wkb(h3_index: str) -> bytes | None:
    """Converte célula H3 em polígono WKB (EPSG:4674)."""
    try:
        import h3
        import shapely.geometry as sg
        import shapely.wkb as swkb
        try:
            boundary = h3.cell_to_boundary(h3_index)  # v4: list[(lat,lng)]
        except AttributeError:
            boundary = h3.h3_to_geo_boundary(h3_index)  # v3
        coords = [(lng, lat) for lat, lng in boundary]
        poly = sg.Polygon(coords)
        return swkb.dumps(poly)
    except Exception as e:
        logger.warning("Falha ao gerar geometria para H3 %s: %s", h3_index, e)
        return None


def _predizer_rf(modelo, X: pd.DataFrame) -> tuple[pd.Series, pd.Series, pd.Series]:
    """
    Predição RF com IC por std entre árvores (90% ≈ ±1.645 std).

    Retorna (pred, ic_lower, ic_upper).
    """
    # Std entre as n_estimators árvores individuais
    tree_preds = np.array([tree.predict(X.values) for tree in modelo.estimators_])
    pred = tree_preds.mean(axis=0)
    std = tree_preds.std(axis=0)

    z = 1.645  # 90% IC
    ic_lower = np.clip(pred - z * std, 0.0, 1.0)
    ic_upper = np.clip(pred + z * std, 0.0, 1.0)

    idx = X.index
    return pd.Series(pred, index=idx), pd.Series(ic_lower, index=idx), pd.Series(ic_upper, index=idx)


def _predizer_lm(modelo, X: pd.DataFrame) -> tuple[pd.Series, pd.Series, pd.Series]:
    """Predição OLS com IC padrão de 90% via statsmodels get_prediction."""
    import statsmodels.api as sm
    X_const = sm.add_constant(X, has_constant="add")
    p = modelo.get_prediction(X_const)
    summary = p.summary_frame(alpha=0.10)  # 90% IC

    idx = X.index
    return (
        pd.Series(summary["mean"].values, index=idx),
        pd.Series(summary["mean_ci_lower"].values, index=idx),
        pd.Series(summary["mean_ci_upper"].values, index=idx),
    )


# ---------------------------------------------------------------------------
# Função pública
# ---------------------------------------------------------------------------

def predizer_h3_t0(
    codigo_ibge: str,
    ano_t0: int,
    resolucao_h3: int,
    db_conn,
    output_dir: Path | None = None,
    modelo: str | None = None,
) -> dict:
    """
    Aplica o modelo da Etapa 3 sobre covariaveis_h3_t0 e persiste estimativas H3.

    Parâmetros
    ----------
    codigo_ibge : str
        Código IBGE de 7 dígitos.
    ano_t0 : int
        Ano-base (ex: 2022).
    resolucao_h3 : int
        Resolução H3 usada na Etapa 2 (ex: 8).
    db_conn : duckdb.DuckDBPyConnection
        Deve conter: covariaveis_h3_t0, modelo_t0_diagnostico.
    output_dir : Path | None
        Pasta com modelo_t0_*.pkl. Default: data/processed/{codigo_ibge}/.

    Retorna
    -------
    dict
        {"status": "ok"|"erro", "n_hexagonos": int, "n_completos": int, "mensagem": str}
    """
    logger.info("[Etapa 4] Predição H3 res=%d — municipio %s, ano %d", resolucao_h3, codigo_ibge, ano_t0)

    presentes = {r[0] for r in db_conn.execute("SHOW TABLES").fetchall()}
    if "covariaveis_h3_t0" not in presentes:
        return {"status": "erro", "mensagem": "covariaveis_h3_t0 ausente (execute Etapa 2 primeiro)."}

    if output_dir is None:
        output_dir = Path(f"data/processed/{codigo_ibge}")
    output_dir = Path(output_dir)

    # Detecta tipo de modelo e carrega
    tipo_modelo = modelo or _detectar_modelo(db_conn)
    logger.info("[Etapa 4] Carregando modelo '%s'", tipo_modelo)
    try:
        modelo = _carregar_modelo(output_dir, codigo_ibge, tipo_modelo)
    except FileNotFoundError as e:
        return {"status": "erro", "mensagem": str(e)}

    # Carrega covariáveis H3
    logger.info("[Etapa 4] Carregando covariaveis_h3_t0")
    df_h3 = db_conn.execute("SELECT * FROM covariaveis_h3_t0").df()

    # Prepara features
    df_h3["fcu_intersecta"] = df_h3["fcu_intersecta"].fillna(False).astype(int)
    df_imp, n_imputed = _imputar_mediana(df_h3, _FEATURES)
    X = df_imp[_FEATURES].copy()
    X.index = df_h3["h3_index"].values

    # Predição
    logger.info("[Etapa 4] Aplicando modelo a %d hexágonos", len(X))
    try:
        if tipo_modelo == "rf":
            pred, ic_lower, ic_upper = _predizer_rf(modelo, X)
        elif tipo_modelo == "lm":
            pred, ic_lower, ic_upper = _predizer_lm(modelo, X)
        else:
            return {"status": "erro", "mensagem": f"Tipo de modelo '{tipo_modelo}' nao suportado para predição."}
    except Exception as e:
        logger.error("[Etapa 4] Erro na predição: %s", e, exc_info=True)
        return {"status": "erro", "mensagem": str(e)}

    # Monta DataFrame de saída
    n_dom = df_h3.set_index("h3_index").get("n_domicilios_grade", pd.Series(dtype=float))

    resultado = pd.DataFrame({
        "h3_index": df_h3["h3_index"].values,
        "h3_resolucao": resolucao_h3,
        "proxy_predito": pred.values,
        "proxy_ic_lower": ic_lower.values,
        "proxy_ic_upper": ic_upper.values,
        "n_domicilios_grade": df_h3.get("n_domicilios_grade", pd.Series(np.nan, index=df_h3.index)).values,
        "n_imputed": n_imputed.values,
    })

    resultado["deficit_estimado"] = resultado["proxy_predito"] * resultado["n_domicilios_grade"]

    # Geometrias H3
    logger.info("[Etapa 4] Gerando geometrias H3")
    resultado["geometry"] = [_h3_cell_polygon_wkb(h) for h in resultado["h3_index"]]

    n_hex = len(resultado)
    n_completos = int((n_imputed == 0).sum())
    deficit_total = resultado["deficit_estimado"].sum()
    logger.info(
        "[Etapa 4] %d hexágonos | %d completos | deficit_estimado_total=%.1f",
        n_hex, n_completos, deficit_total,
    )

    # Persiste no DuckDB
    db_conn.execute("DROP TABLE IF EXISTS deficit_predito_h3_t0")
    db_conn.execute("CREATE TABLE deficit_predito_h3_t0 AS SELECT * FROM resultado")
    logger.info("[Etapa 4] deficit_predito_h3_t0 salvo: %d linhas", n_hex)

    return {
        "status": "ok",
        "n_hexagonos": n_hex,
        "n_completos": n_completos,
        "deficit_estimado_total": round(float(deficit_total), 1),
        "mensagem": f"deficit_predito_h3_t0: {n_hex} hexágonos, deficit_total={deficit_total:.0f}.",
    }
