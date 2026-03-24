"""
grupo5_pnadc.py — Coleta de dados da PNADc via R + PNADcIBGE.

⚠️ LIMITAÇÃO FUNDAMENTAL: A PNADc NÃO tem representatividade municipal.
   A menor unidade geográfica é a ÁREA DE PONDERAÇÃO (identificada via geobr).
   Os resultados são válidos para a área de ponderação, não para o município.

Fluxo: R (PNADcIBGE + survey + geobr) → CSV → Python → DuckDB

Refatorado de: h3_jacarei/src/coleta/pnadc.py
    Reaproveitado: _localizar_rscript(), padrão subprocess.run, cache check
    Reescrito: filtro genérico por município (não hardcoded para SP/Jacareí),
               variáveis habitacionais S01xxx, saída em DuckDB

Tabelas DuckDB geradas:
    pnadc_estimativas  — svymean por variável (estimativa, SE, IC, n_obs)
    pnadc_metadados    — área de ponderação usada, variáveis ausentes, avisos

Dependências: utils/db_utils.py, r_scripts/extrair_pnadc.R
"""

from __future__ import annotations

import glob
import logging
import shutil
import subprocess
from pathlib import Path

import geopandas as gpd
import pandas as pd

from ..utils.db_utils import salvar_dataframe

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers (refatorados de h3_jacarei/src/coleta/pnadc.py)
# ---------------------------------------------------------------------------

def _localizar_rscript() -> str:
    """
    Localiza o executável Rscript no sistema operacional.

    Tenta em ordem:
    1. shutil.which() — funciona se R está no PATH do sistema
    2. Busca direta em C:/Program Files/R/ (Windows, instalação padrão)

    Refatorado de: h3_jacarei/src/coleta/pnadc.py — copiado sem alterações.
    """
    rscript = shutil.which("Rscript")
    if rscript:
        return rscript

    candidatos = glob.glob("C:/Program Files/R/*/bin/Rscript.exe")
    if candidatos:
        return sorted(candidatos)[-1]

    raise RuntimeError(
        "Rscript não encontrado no PATH nem em C:/Program Files/R/.\n"
        "Para corrigir:\n"
        "  1. Instale o R: https://cran.r-project.org/\n"
        "  2. Durante a instalação, marque 'Add R to PATH'\n"
        "  3. Reinicie o terminal/VS Code para atualizar o PATH\n"
        "  4. Teste: abra o terminal e digite 'Rscript --version'"
    )


# ---------------------------------------------------------------------------
# Função principal do grupo
# ---------------------------------------------------------------------------

def coletar_grupo5(
    codigo_ibge: str,
    limite_municipal: gpd.GeoDataFrame,
    output_dir: Path,
    db_conn,
    ano: int = 2022,
    trimestre: int = 4,
    forcar: bool = False,
    **kwargs,
) -> dict:
    """
    Coleta estimativas PNADc para a área de ponderação do município.

    O script R (r_scripts/extrair_pnadc.R) é responsável por:
    - Identificar a área de ponderação via geobr::read_weighting_area()
    - Baixar os microdados PNADc do IBGE
    - Filtrar pelo código V1029 da área de ponderação
    - Calcular svymean com plano amostral correto
    - Exportar CSV de estimativas e metadados

    Esta função orquestra a chamada ao R via subprocess e persiste os
    resultados no DuckDB.

    Parâmetros
    ----------
    codigo_ibge : str
        Código IBGE de 7 dígitos. Ex: "2701407".
    limite_municipal : gpd.GeoDataFrame
        Não usado diretamente — a área de ponderação é identificada via
        geobr no script R a partir do código IBGE.
    output_dir : Path
        Pasta de saída: data/raw/{uf}_{municipio}/pnadc/
    db_conn : duckdb.DuckDBPyConnection
        Conexão aberta com o DuckDB do município.
    ano : int
        Ano da PNADc. Padrão: 2022.
    trimestre : int
        Trimestre (1-4). Padrão: 4.
    forcar : bool
        Se True, reexecuta o R mesmo que os CSVs já existam.

    Retorna
    -------
    dict
        {"status": "ok"|"erro", "camadas": [...], "mensagem": "..."}

    Notas
    -----
    - Na primeira execução o R baixa os dados do IBGE (~5-15 min).
    - O CSV é cacheado em output_dir — execuções seguintes pulam o R.
    - Variáveis S01xxx (suplemento habitacional) podem estar ausentes em
      alguns trimestres; o script R registra isso nos metadados sem erro fatal.
    - O aviso de limitação geográfica é gravado na tabela pnadc_metadados.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    csv_est  = output_dir / f"pnadc_{ano}T{trimestre}_estimativas.csv"
    csv_meta = output_dir / f"pnadc_{ano}T{trimestre}_metadados.csv"

    camadas_salvas = []

    try:
        # -----------------------------------------------------------------
        # 1. Executar script R (com cache)
        # -----------------------------------------------------------------
        if csv_est.exists() and not forcar:
            logger.info(
                "[Grupo 5] CSV já existe (%s) — pulando execução R. "
                "Use forcar=True para reexecutar.",
                csv_est.name,
            )
        else:
            rscript = _localizar_rscript()
            script_r = Path(__file__).parent.parent / "r_scripts" / "extrair_pnadc.R"

            if not script_r.exists():
                raise FileNotFoundError(
                    f"Script R não encontrado: {script_r}\n"
                    "Verifique se r_scripts/extrair_pnadc.R existe no módulo."
                )

            logger.info(
                "[Grupo 5] Executando R: %s (pode demorar 5-15 min na 1ª vez)...",
                script_r.name,
            )

            resultado = subprocess.run(
                [
                    rscript, str(script_r),
                    "--codigo_ibge", codigo_ibge,
                    "--ano",         str(ano),
                    "--trimestre",   str(trimestre),
                    "--output_dir",  str(output_dir),
                ],
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
            )

            # Exibir output completo do R no log
            saida_r = (resultado.stdout or "") + (resultado.stderr or "")
            for linha in saida_r.splitlines():
                logger.info("[R] %s", linha)

            if resultado.returncode != 0:
                raise RuntimeError(
                    f"Script R falhou (código {resultado.returncode}).\n"
                    "Verifique o log acima.\n"
                    "Causas comuns:\n"
                    "  - Pacotes R ausentes (PNADcIBGE, survey, geobr, arrow)\n"
                    "  - Sem conexão com a internet\n"
                    "  - Município sem área de ponderação no geobr"
                )

            if not csv_est.exists():
                raise RuntimeError(
                    f"R executou sem erros, mas {csv_est.name} não foi gerado.\n"
                    "Verifique o log do R acima."
                )

        # -----------------------------------------------------------------
        # 2. Ler CSV de estimativas → DuckDB
        # -----------------------------------------------------------------
        logger.info("[Grupo 5] Lendo estimativas de %s...", csv_est.name)
        df_est = pd.read_csv(csv_est, encoding="utf-8")

        if df_est.empty:
            raise ValueError("CSV de estimativas está vazio.")

        salvar_dataframe(db_conn, df_est, "pnadc_estimativas")
        camadas_salvas.append("pnadc_estimativas")
        logger.info(
            "[Grupo 5] pnadc_estimativas: %d variáveis, colunas: %s",
            len(df_est),
            list(df_est.columns),
        )

        # -----------------------------------------------------------------
        # 3. Ler CSV de metadados → DuckDB
        # -----------------------------------------------------------------
        if csv_meta.exists():
            df_meta = pd.read_csv(csv_meta, encoding="utf-8")
            salvar_dataframe(db_conn, df_meta, "pnadc_metadados")
            camadas_salvas.append("pnadc_metadados")

            # Exibir aviso de limitação geográfica no log
            aviso = df_meta.loc[df_meta["chave"] == "aviso", "valor"]
            if not aviso.empty:
                logger.warning("[Grupo 5] %s", aviso.iloc[0])

            areas = df_meta.loc[
                df_meta["chave"] == "areas_ponderacao_nomes", "valor"
            ]
            if not areas.empty:
                logger.info("[Grupo 5] Área(s) de ponderação: %s", areas.iloc[0])

        return {
            "status": "ok",
            "camadas": camadas_salvas,
            "mensagem": (
                f"PNADc {ano}T{trimestre} processada: "
                f"{len(df_est)} variáveis estimadas."
            ),
        }

    except Exception as exc:
        logger.error("[Grupo 5] Erro: %s", exc, exc_info=True)
        return {
            "status": "erro",
            "camadas": camadas_salvas,
            "mensagem": str(exc),
        }
