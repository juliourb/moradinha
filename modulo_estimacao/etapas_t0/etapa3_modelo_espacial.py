"""
etapa3_modelo_espacial.py — Ajuste do modelo proxy_carencias_setor ~ covariáveis.

Aprende a relação entre as covariáveis territoriais observáveis (Etapa 2) e o
índice composto de carências habitacionais por setor (Etapa 1), permitindo
predição para H3 e para outros municípios.

Modelos suportados:
    'rf'  — Random Forest Regressor (padrão). Robusto a multicolinearidade e
            não-linearidade. Modelo principal para predição.
    'lm'  — OLS com erros HC3 (statsmodels). Interpretável; para comparação
            de coeficientes e diagnóstico de multicolinearidade (VIF).
    'gwr' — Geographically Weighted Regression (mgwr). Permite coeficientes
            variantes no espaço. Opcional — requer mgwr instalado.

Saídas:
    modelo_t0_diagnostico (tabela DuckDB): métricas e diagnósticos
    data/processed/{municipio}/modelo_t0_{modelo}.pkl: modelo serializado
"""

from __future__ import annotations

import json
import logging
import pickle
from datetime import datetime, timezone
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import shapely.wkb as swkb

logger = logging.getLogger(__name__)

_MODELOS_SUPORTADOS = frozenset({"lm", "rf", "gwr"})

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

_TARGET = "proxy_carencias_setor"


# ---------------------------------------------------------------------------
# Preparação do dataset
# ---------------------------------------------------------------------------

def _preparar_dataset(db_conn) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Junta proxy_setor + covariaveis_setor_t0, elimina NaN, retorna (df_treino, df_completo).

    df_treino: apenas setores com todos os valores preenchidos (para ajuste do modelo)
    df_completo: todos os setores (para predição posterior na Etapa 4)
    """
    proxy = db_conn.execute(
        f"SELECT cod_setor, {_TARGET} FROM proxy_setor"
    ).df()

    cov_cols = ", ".join(["cod_setor"] + _FEATURES)
    cov = db_conn.execute(f"SELECT {cov_cols} FROM covariaveis_setor_t0").df()

    df = proxy.merge(cov, on="cod_setor", how="inner")

    # Converte booleano para int
    df["fcu_intersecta"] = df["fcu_intersecta"].fillna(False).astype(int)

    df_treino = df.dropna(subset=[_TARGET] + _FEATURES).copy()
    return df_treino, df


# ---------------------------------------------------------------------------
# Pesos espaciais e diagnóstico de Moran
# ---------------------------------------------------------------------------

def _carregar_gdf_setores(db_conn, cod_setores: list[str]) -> gpd.GeoDataFrame:
    """Carrega geometria dos setores de treinamento como GeoDataFrame."""
    cod_setores_sql = ", ".join(f"'{c}'" for c in cod_setores)
    rows = db_conn.execute(
        f"SELECT LEFT(CD_SETOR, 15) AS cod_setor, ST_AsWKB(geometry) AS geometry "
        f"FROM setores_censitarios WHERE LEFT(CD_SETOR, 15) IN ({cod_setores_sql})"
    ).df()
    geoms = rows["geometry"].apply(lambda b: swkb.loads(bytes(b)) if b is not None else None)
    return gpd.GeoDataFrame(rows[["cod_setor"]], geometry=geoms, crs="EPSG:4674")


def _calcular_moran_i(residuos: pd.Series, gdf: gpd.GeoDataFrame, cod_col: str = "cod_setor"):
    """
    Calcula Moran's I global nos resíduos do modelo.

    Retorna (moran_i, p_value) ou (NaN, NaN) se esda/libpysal não disponível.
    """
    try:
        import esda
        import libpysal
    except ImportError:
        logger.warning("[Etapa 3] esda/libpysal nao instalados — Moran's I ignorado")
        return np.nan, np.nan

    # Garante alinhamento
    gdf_aligned = gdf.set_index(cod_col).loc[residuos.index]

    try:
        w = libpysal.weights.Queen.from_dataframe(gdf_aligned, use_index=True, silence_warnings=True)
        w.transform = "r"
        mi = esda.Moran(residuos.values, w)
        return float(mi.I), float(mi.p_sim)
    except Exception as e:
        logger.warning("[Etapa 3] Moran's I falhou: %s", e)
        return np.nan, np.nan


# ---------------------------------------------------------------------------
# Ajuste dos modelos
# ---------------------------------------------------------------------------

def _ajustar_rf(X: pd.DataFrame, y: pd.Series, output_dir: Path, cod_mun: str) -> dict:
    from sklearn.ensemble import RandomForestRegressor
    from sklearn.model_selection import cross_val_score

    rf = RandomForestRegressor(
        n_estimators=300,
        max_features="sqrt",
        min_samples_leaf=3,
        random_state=42,
        n_jobs=-1,
    )
    rf.fit(X, y)

    y_pred = pd.Series(rf.predict(X), index=y.index)
    residuos = y - y_pred

    # CV-RMSE (5-fold) para estimativa menos otimista
    cv_scores = cross_val_score(rf, X, y, cv=5, scoring="neg_root_mean_squared_error", n_jobs=-1)
    cv_rmse = float(-cv_scores.mean())

    r2_treino = float(rf.score(X, y))
    rmse_treino = float(np.sqrt(((y - y_pred) ** 2).mean()))

    # Feature importances
    importancias = pd.Series(rf.feature_importances_, index=X.columns).sort_values(ascending=False)

    # Serializa
    pkl_path = output_dir / f"modelo_t0_rf_{cod_mun}.pkl"
    with open(pkl_path, "wb") as f:
        pickle.dump(rf, f)
    logger.info("[Etapa 3] RF serializado: %s", pkl_path)

    return {
        "modelo_obj": rf,
        "y_pred": y_pred,
        "residuos": residuos,
        "r2_treino": r2_treino,
        "rmse_treino": rmse_treino,
        "cv_rmse_5fold": cv_rmse,
        "importancias": importancias.to_dict(),
        "pkl_path": str(pkl_path),
    }


def _ajustar_lm(X: pd.DataFrame, y: pd.Series, output_dir: Path, cod_mun: str) -> dict:
    import statsmodels.api as sm

    X_const = sm.add_constant(X)
    ols = sm.OLS(y, X_const).fit(cov_type="HC3")

    y_pred = pd.Series(ols.fittedvalues, index=y.index)
    residuos = pd.Series(ols.resid, index=y.index)

    r2_treino = float(ols.rsquared)
    rmse_treino = float(np.sqrt((residuos ** 2).mean()))

    # VIF
    try:
        from statsmodels.stats.outliers_influence import variance_inflation_factor
        vif = {
            col: float(variance_inflation_factor(X_const.values, i))
            for i, col in enumerate(X_const.columns)
            if col != "const"
        }
    except Exception:
        vif = {}

    pkl_path = output_dir / f"modelo_t0_lm_{cod_mun}.pkl"
    with open(pkl_path, "wb") as f:
        pickle.dump(ols, f)
    logger.info("[Etapa 3] LM serializado: %s", pkl_path)

    return {
        "modelo_obj": ols,
        "y_pred": y_pred,
        "residuos": residuos,
        "r2_treino": r2_treino,
        "rmse_treino": rmse_treino,
        "cv_rmse_5fold": np.nan,  # não aplicado para LM
        "vif": vif,
        "pkl_path": str(pkl_path),
    }


def _ajustar_gwr(X: pd.DataFrame, y: pd.Series, coords: np.ndarray, output_dir: Path, cod_mun: str) -> dict:
    try:
        from mgwr.gwr import GWR
        from mgwr.sel_bw import Sel_BW
    except ImportError:
        raise ImportError("mgwr nao instalado. Execute: pip install mgwr")

    X_arr = X.values
    y_arr = y.values.reshape(-1, 1)

    # Seleção automática de bandwidth via AICc
    selector = Sel_BW(coords, y_arr, X_arr)
    bw = selector.search()

    gwr_model = GWR(coords, y_arr, X_arr, bw).fit()

    y_pred = pd.Series(gwr_model.predy.flatten(), index=y.index)
    residuos = y - y_pred

    r2_treino = float(gwr_model.R2)
    rmse_treino = float(np.sqrt(((y - y_pred) ** 2).mean()))

    pkl_path = output_dir / f"modelo_t0_gwr_{cod_mun}.pkl"
    with open(pkl_path, "wb") as f:
        pickle.dump({"bw": bw, "resultado": gwr_model}, f)

    return {
        "modelo_obj": gwr_model,
        "y_pred": y_pred,
        "residuos": residuos,
        "r2_treino": r2_treino,
        "rmse_treino": rmse_treino,
        "cv_rmse_5fold": np.nan,
        "pkl_path": str(pkl_path),
    }


# ---------------------------------------------------------------------------
# Função pública
# ---------------------------------------------------------------------------

def ajustar_modelo_t0(
    codigo_ibge: str,
    ano_t0: int,
    db_conn,
    modelo: str = "rf",
    output_dir: Path | None = None,
) -> dict:
    """
    Ajusta modelo proxy_carencias_setor ~ covariáveis e persiste diagnósticos.

    Parâmetros
    ----------
    codigo_ibge : str
        Código IBGE de 7 dígitos. Ex: "2700300".
    ano_t0 : int
        Ano-base (ex: 2022).
    db_conn : duckdb.DuckDBPyConnection
        Deve conter: proxy_setor, covariaveis_setor_t0, setores_censitarios.
    modelo : str
        'rf' (padrão), 'lm', ou 'gwr'.
    output_dir : Path | None
        Pasta para .pkl. Default: data/processed/{codigo_ibge}/.

    Retorna
    -------
    dict
        {"status": "ok"|"erro", "r2": float, "rmse": float,
         "cv_rmse_5fold": float, "moran_i": float, "moran_p": float,
         "n_setores": int, "pkl_path": str, "mensagem": str}
    """
    if modelo not in _MODELOS_SUPORTADOS:
        return {"status": "erro", "mensagem": f"modelo '{modelo}' nao suportado. Use: {_MODELOS_SUPORTADOS}"}

    logger.info("[Etapa 3] Ajustando modelo '%s' — municipio %s, ano %d", modelo, codigo_ibge, ano_t0)

    # Verificar dependencias
    presentes = {r[0] for r in db_conn.execute("SHOW TABLES").fetchall()}
    for t in ["proxy_setor", "covariaveis_setor_t0"]:
        if t not in presentes:
            return {"status": "erro", "mensagem": f"Tabela {t} ausente."}

    # Preparar dados
    df_treino, _ = _preparar_dataset(db_conn)
    n_setores = len(df_treino)
    logger.info("[Etapa 3] Dataset de treino: %d setores", n_setores)

    if n_setores < 30:
        return {"status": "erro", "mensagem": f"Setores de treino insuficientes: {n_setores} (mínimo 30)."}

    # Usa cod_setor como índice para alinhamento em residuos e Moran's I
    df_treino = df_treino.set_index("cod_setor")
    X = df_treino[_FEATURES].copy()
    y = df_treino[_TARGET].copy()

    # Diretório de output
    if output_dir is None:
        output_dir = Path(f"data/processed/{codigo_ibge}")
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Ajuste
    try:
        if modelo == "rf":
            res = _ajustar_rf(X, y, output_dir, codigo_ibge)
        elif modelo == "lm":
            res = _ajustar_lm(X, y, output_dir, codigo_ibge)
        elif modelo == "gwr":
            gdf = _carregar_gdf_setores(db_conn, list(df_treino["cod_setor"]))
            gdf_proj = gdf.set_index("cod_setor").loc[df_treino["cod_setor"].values].to_crs("EPSG:5880")
            centroids = gdf_proj.geometry.centroid
            coords = np.column_stack([centroids.x, centroids.y])
            res = _ajustar_gwr(X, y, coords, output_dir, codigo_ibge)
    except Exception as e:
        logger.error("[Etapa 3] Erro no ajuste do modelo: %s", e, exc_info=True)
        return {"status": "erro", "mensagem": str(e)}

    # Moran's I nos resíduos
    logger.info("[Etapa 3] Calculando Moran's I nos residuos")
    gdf_treino = _carregar_gdf_setores(db_conn, list(df_treino.index))
    moran_i, moran_p = _calcular_moran_i(res["residuos"], gdf_treino)

    y_mean = float(y.mean())
    cv_rmse_y = float(res["rmse_treino"] / y_mean) if y_mean > 0 else np.nan

    logger.info(
        "[Etapa 3] R²=%.4f | RMSE=%.4f | CV(RMSE)=%.3f | Moran's I=%.4f (p=%.3f)",
        res["r2_treino"], res["rmse_treino"], cv_rmse_y,
        moran_i if not np.isnan(moran_i) else -99,
        moran_p if not np.isnan(moran_p) else -99,
    )

    # Persistir diagnósticos no DuckDB
    extras = {}
    if "importancias" in res:
        extras["feature_importances"] = json.dumps(res["importancias"])
    if "vif" in res:
        extras["vif"] = json.dumps(res["vif"])

    diag = pd.DataFrame([{
        "modelo": modelo,
        "r2_treino": res["r2_treino"],
        "rmse_treino": res["rmse_treino"],
        "cv_rmse_y": cv_rmse_y,
        "cv_rmse_5fold": res.get("cv_rmse_5fold", np.nan),
        "moran_i_residuos": moran_i,
        "moran_p": moran_p,
        "n_setores": n_setores,
        "features_usadas": json.dumps(_FEATURES),
        "ano_t0": ano_t0,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        **extras,
    }])

    db_conn.execute("DROP TABLE IF EXISTS modelo_t0_diagnostico")
    db_conn.execute("CREATE TABLE modelo_t0_diagnostico AS SELECT * FROM diag")
    logger.info("[Etapa 3] modelo_t0_diagnostico salvo no DuckDB")

    return {
        "status": "ok",
        "modelo": modelo,
        "r2": res["r2_treino"],
        "rmse": res["rmse_treino"],
        "cv_rmse_y": cv_rmse_y,
        "cv_rmse_5fold": res.get("cv_rmse_5fold", np.nan),
        "moran_i": moran_i,
        "moran_p": moran_p,
        "n_setores": n_setores,
        "pkl_path": res["pkl_path"],
        "mensagem": f"Modelo '{modelo}' ajustado: R²={res['r2_treino']:.4f}, RMSE={res['rmse_treino']:.4f}.",
    }
