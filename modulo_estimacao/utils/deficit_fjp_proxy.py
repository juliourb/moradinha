"""
deficit_fjp_proxy.py — Helpers de baixo nível para o cálculo do proxy_carencias_setor.

Usadas pela Etapa 1 (proxy_setor). Isoladas aqui para facilitar testes.

Referência: FJP (2021). Déficit Habitacional no Brasil 2016–2019.
    Cap. 2: Metodologia. Fundação João Pinheiro, Belo Horizonte.

Nota de schema (censo 2022 — Universo):
    censo_domicilio01 : V00001..V00089  (espécie, tipo, número de moradores)
    censo_domicilio02 : V00090..V00495  (infraestrutura: água, esgoto, lixo, banheiro)

    V00238 (sem banheiro) está em censo_domicilio02 apesar de o mapeamento FJP inicial
    indicar domicilio01. Confirmado via DESCRIBE no DuckDB de Arapiraca em 2026-04-27.
"""

from __future__ import annotations

import numpy as np
import pandas as pd


# ---------------------------------------------------------------------------
# Helpers de conversão
# ---------------------------------------------------------------------------

def to_numeric_br(series: pd.Series) -> pd.Series:
    """
    Converte coluna string no formato BR (vírgula decimal, 'X' = supressão IBGE)
    para float64.

    'X' e valores ausentes viram NaN.
    """
    s = series.astype(str).str.strip()
    s = s.replace({"X": np.nan, "x": np.nan, "": np.nan, "nan": np.nan, "None": np.nan})
    s = s.str.replace(",", ".", regex=False)
    return pd.to_numeric(s, errors="coerce")


# ---------------------------------------------------------------------------
# Cálculo das proporções de carência
# ---------------------------------------------------------------------------

_VARIAVEIS_D1 = {
    "n_dom_total": "V00001",        # total DPP ocupados (denominador)
    "n_improvisados": "V00002",     # domicílios improvisados (A2 FJP)
    "n_comodos_cortico": "V00050",  # cômodos/cortiço (B1 FJP)
}

_VARIAVEIS_D2 = {
    "n_sem_banheiro": "V00238",     # sem banheiro nem sanitário
    "n_com_agua_rede": "V00111",    # com abastecimento de água via rede geral
    "n_com_esgoto_rede": "V00309",  # com esgoto via rede geral ou pluvial
    "n_com_lixo_coletado": "V00397",  # com lixo coletado no domicílio
}


def proporcoes_carencias_setor(
    domicilio01: pd.DataFrame,
    domicilio02: pd.DataFrame,
) -> pd.DataFrame:
    """
    Calcula as 6 proporções de carência habitacional por setor censitário.

    Parâmetros
    ----------
    domicilio01 : pd.DataFrame
        Tabela censo_domicilio01 com colunas Cod_setor, V00001, V00002, V00050.
    domicilio02 : pd.DataFrame
        Tabela censo_domicilio02 com colunas Cod_setor, V00238, V00111, V00309, V00397.

    Retorna
    -------
    pd.DataFrame
        Uma linha por setor com colunas:
            cod_setor, n_dom_total, n_improvisados, n_comodos_cortico,
            n_sem_banheiro, n_com_agua_rede, n_com_esgoto_rede, n_com_lixo_coletado,
            prop_improvisados, prop_comodos_cortico, prop_sem_banheiro,
            prop_sem_agua_rede, prop_sem_esgoto_rede, prop_sem_lixo_coletado
    """
    # Selecionar e converter colunas de domicilio01
    cols_d1 = {"Cod_setor": "cod_setor"}
    cols_d1.update({v: k for k, v in _VARIAVEIS_D1.items()})
    d1 = domicilio01[list(cols_d1.keys())].rename(columns=cols_d1).copy()
    for col in list(_VARIAVEIS_D1.keys()):
        d1[col] = to_numeric_br(d1[col])

    # Selecionar e converter colunas de domicilio02
    cols_d2 = {"Cod_setor": "cod_setor"}
    cols_d2.update({v: k for k, v in _VARIAVEIS_D2.items()})
    d2 = domicilio02[list(cols_d2.keys())].rename(columns=cols_d2).copy()
    for col in list(_VARIAVEIS_D2.keys()):
        d2[col] = to_numeric_br(d2[col])

    df = d1.merge(d2, on="cod_setor", how="outer")

    denom = df["n_dom_total"].replace(0, np.nan)

    df["prop_improvisados"] = df["n_improvisados"] / denom
    df["prop_comodos_cortico"] = df["n_comodos_cortico"] / denom
    df["prop_sem_banheiro"] = df["n_sem_banheiro"] / denom
    df["prop_sem_agua_rede"] = 1.0 - df["n_com_agua_rede"] / denom
    df["prop_sem_esgoto_rede"] = 1.0 - df["n_com_esgoto_rede"] / denom
    df["prop_sem_lixo_coletado"] = 1.0 - df["n_com_lixo_coletado"] / denom

    # Clipa proporções em [0, 1] para corrigir inconsistências dos dados brutos
    for col in [c for c in df.columns if c.startswith("prop_")]:
        df[col] = df[col].clip(0.0, 1.0)

    return df


_PROPS_CARENCIA = [
    "prop_improvisados",
    "prop_comodos_cortico",
    "prop_sem_banheiro",
    "prop_sem_agua_rede",
    "prop_sem_esgoto_rede",
    "prop_sem_lixo_coletado",
]


def proxy_carencias_igual(df: pd.DataFrame) -> pd.Series:
    """
    Calcula proxy_carencias_setor com pesos iguais (1/6 cada componente).

    Setores com algum componente NaN recebem média das proporções disponíveis.
    Setores com todos os componentes NaN (ex: setor vazio) ficam como NaN.

    Parâmetros
    ----------
    df : pd.DataFrame
        DataFrame com as 6 colunas prop_*.

    Retorna
    -------
    pd.Series
        proxy_carencias_setor por setor.
    """
    return df[_PROPS_CARENCIA].mean(axis=1, skipna=True)
