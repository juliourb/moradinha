"""
ipf.py — Iterative Proportional Fitting (raking) para calibração de estimativas.

IPF é um algoritmo de ajuste iterativo que, dado um array de estimativas e um
conjunto de totais marginais conhecidos, ajusta as estimativas para que cada
marginal seja satisfeita, iterando até convergência.

Intuição: imagine uma tabela de contingência onde você conhece os totais de linha
e coluna (marginais) mas não os valores internos. O IPF começa com qualquer
tabela inicial e alterna entre:
    1. Escalar cada linha para bater com o total de linha correspondente.
    2. Escalar cada coluna para bater com o total de coluna correspondente.
Repete até que as diferenças entre os marginais alcançados e os desejados sejam
menores que a tolerância.

Na Etapa 5 do moradinha:
    - "Linhas" são setores; "colunas" são hexágonos H3 dentro de cada setor.
    - Marginais de linha = proxy_setor do Censo (Etapa 1) — controle fino.
    - Marginais de coluna = estimativa PNADc do domínio (Grupo 5) — controle macro.

Referência: Deming & Stephan (1940). On a Least Squares Adjustment of a Sampled
    Frequency Table When the Expected Marginal Totals Are Known. Ann. Math. Stat.
"""

from __future__ import annotations

import logging

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


def ipf_1d(
    valores: pd.Series,
    alvo: float,
    nome_nivel: str = "nível",
) -> pd.Series:
    """
    IPF trivial de um único nível: escala todos os valores por um fator comum.

    Usado quando há apenas uma âncora (ex: calibrar H3 para o total do domínio
    sem âncora setor-a-setor).

    Parâmetros
    ----------
    valores : pd.Series
        Estimativas brutas a calibrar.
    alvo : float
        Total desejado após calibração.
    nome_nivel : str
        Nome para mensagens de log.

    Retorna
    -------
    pd.Series
        Valores calibrados com a mesma soma que `alvo`.
    """
    total_atual = valores.sum()
    if total_atual == 0 or np.isnan(total_atual):
        logger.warning("[IPF-1d] Total atual = 0 ou NaN em '%s' — sem calibração", nome_nivel)
        return valores.copy()
    fator = alvo / total_atual
    logger.info("[IPF-1d] %s: fator=%.4f (alvo=%.1f, atual=%.1f)", nome_nivel, fator, alvo, total_atual)
    return valores * fator


def ipf_2d(
    estimativas_h3: pd.DataFrame,
    totais_setor: pd.Series,
    totais_dominio: pd.Series,
    coluna_setor: str = "cod_setor",
    coluna_dominio: str = "cod_dominio",
    coluna_valor: str = "deficit_predito",
    tolerancia: float = 1e-4,
    max_iter: int = 50,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    IPF em dois níveis para calibrar estimativas H3.

    Nível 1: para cada setor, escala os H3 dentro do setor para bater com
             totais_setor[setor].
    Nível 2: para cada domínio PNADc, escala os H3 dentro do domínio para
             bater com totais_dominio[dominio].
    Itera Nível 1 → Nível 2 até convergência.

    Parâmetros
    ----------
    estimativas_h3 : pd.DataFrame
        Uma linha por H3, colunas: h3_index, cod_setor, cod_dominio, deficit_predito.
        H3 sem cod_setor (NaN) são mantidos mas não participam do Nível 1.
    totais_setor : pd.Series
        Index = cod_setor, values = total desejado por setor (proxy_total_sem_onus).
        Apenas setores com pelo menos um H3 associado participam do IPF.
    totais_dominio : pd.Series
        Index = cod_dominio, values = total desejado por domínio (PNADc t0).
    tolerancia : float
        Critério de parada: max(|fator - 1|) < tolerancia.
    max_iter : int
        Limite de iterações.

    Retorna
    -------
    (estimativas_calibradas, convergencia_log)
        estimativas_calibradas: DataFrame com coluna deficit_calibrado e
            fator_calibracao_setor, fator_calibracao_dominio adicionadas.
        convergencia_log: DataFrame com colunas iteracao, delta_setor, delta_dominio.
    """
    df = estimativas_h3.copy()
    df["deficit_calibrado"] = df[coluna_valor].astype(float)
    df["fator_calibracao_setor"] = 1.0
    df["fator_calibracao_dominio"] = 1.0

    mask_setor = df[coluna_setor].notna()
    mask_dominio = df[coluna_dominio].notna()

    log_rows: list[dict] = []
    convergiu = False

    for iteracao in range(1, max_iter + 1):
        # --- Nível 1: por setor ---
        soma_setor = (
            df.loc[mask_setor]
            .groupby(coluna_setor)["deficit_calibrado"]
            .sum()
        )
        # Fatores apenas para setores presentes nos dados
        setores_presentes = soma_setor.index.intersection(totais_setor.index)
        fatores_setor = pd.Series(1.0, index=df.index)
        for setor in setores_presentes:
            atual = soma_setor.get(setor, 0.0)
            if atual > 0:
                fator = totais_setor[setor] / atual
            else:
                fator = 1.0
            mask_s = mask_setor & (df[coluna_setor] == setor)
            fatores_setor.loc[mask_s] = fator

        df.loc[mask_setor, "deficit_calibrado"] *= fatores_setor.loc[mask_setor]
        df.loc[mask_setor, "fator_calibracao_setor"] *= fatores_setor.loc[mask_setor]
        delta_setor = (fatores_setor.loc[mask_setor] - 1.0).abs().max()

        # --- Nível 2: por domínio ---
        soma_dominio = (
            df.loc[mask_dominio]
            .groupby(coluna_dominio)["deficit_calibrado"]
            .sum()
        )
        fatores_dominio = pd.Series(1.0, index=df.index)
        for dom in soma_dominio.index.intersection(totais_dominio.index):
            atual = soma_dominio.get(dom, 0.0)
            if atual > 0:
                fator = totais_dominio[dom] / atual
            else:
                fator = 1.0
            mask_d = mask_dominio & (df[coluna_dominio] == dom)
            fatores_dominio.loc[mask_d] = fator

        df.loc[mask_dominio, "deficit_calibrado"] *= fatores_dominio.loc[mask_dominio]
        df.loc[mask_dominio, "fator_calibracao_dominio"] *= fatores_dominio.loc[mask_dominio]
        delta_dominio = (fatores_dominio.loc[mask_dominio] - 1.0).abs().max()

        log_rows.append({
            "iteracao": iteracao,
            "delta_max_setor": round(float(delta_setor), 6),
            "delta_max_dominio": round(float(delta_dominio), 6),
        })
        logger.debug(
            "[IPF-2d] iter=%d | delta_setor=%.6f | delta_dominio=%.6f",
            iteracao, delta_setor, delta_dominio,
        )

        if max(delta_setor, delta_dominio) < tolerancia:
            convergiu = True
            logger.info("[IPF-2d] Convergiu em %d iterações (tol=%.1e)", iteracao, tolerancia)
            break

    if not convergiu:
        logger.warning("[IPF-2d] Não convergiu em %d iterações (delta final: setor=%.4f, dom=%.4f)",
                       max_iter, delta_setor, delta_dominio)

    convergencia_log = pd.DataFrame(log_rows)
    return df, convergencia_log
