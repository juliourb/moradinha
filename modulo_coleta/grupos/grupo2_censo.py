"""
grupo2_censo.py — Coleta de dados tabulares do Censo 2022.

Arquivos coletados do FTP IBGE (nacionais, filtrados por CD_SETOR):
    - domicilio01   → caracteristicas_domicilio1_BR   (características físicas)
    - domicilio02   → caracteristicas_domicilio2_BR   (acesso a serviços)
    - responsavel01 → renda_responsavel_BR_csv        (rendimento do responsável)

URL bases:
    domicilio01/02:
        ftp.ibge.gov.br/.../Agregados_por_Setores_Censitarios/Agregados_por_Setor_csv/
    responsavel01:
        ftp.ibge.gov.br/.../Agregados_por_Setores_Censitarios_Rendimento_do_Responsavel/

Encoding dos CSVs: latin-1, separador: ;
Filtro por município: primeiros 7 dígitos de Cod_setor = codigo_ibge

Tabelas DuckDB geradas:
    censo_domicilio01, censo_domicilio02, censo_responsavel01

Dependências: utils/ibge_ftp.py, utils/db_utils.py
"""

from __future__ import annotations

import csv
import io
import logging
import zipfile
from pathlib import Path

import geopandas as gpd
import pandas as pd

from ..utils.db_utils import salvar_dataframe
from ..utils.ibge_ftp import baixar_arquivo, buscar_zip_no_ftp

logger = logging.getLogger(__name__)

_URL_BASE_DOMICILIO = (
    "https://ftp.ibge.gov.br/Censos/Censo_Demografico_2022"
    "/Agregados_por_Setores_Censitarios/Agregados_por_Setor_csv/"
)
_URL_BASE_RESPONSAVEL = (
    "https://ftp.ibge.gov.br/Censos/Censo_Demografico_2022"
    "/Agregados_por_Setores_Censitarios_Rendimento_do_Responsavel/"
)

# Mapeamento: nome_tabela → (url_base, prefixo_zip)
_ARQUIVOS_CENSO: dict[str, tuple[str, str]] = {
    "censo_domicilio01": (
        _URL_BASE_DOMICILIO,
        "Agregados_por_setores_caracteristicas_domicilio1_BR",
    ),
    "censo_domicilio02": (
        _URL_BASE_DOMICILIO,
        "Agregados_por_setores_caracteristicas_domicilio2_BR",
    ),
    "censo_responsavel01": (
        _URL_BASE_RESPONSAVEL,
        "Agregados_por_setores_renda_responsavel_BR_csv",
    ),
}


def _baixar_e_filtrar_csv(
    codigo_ibge: str,
    url_base: str,
    prefixo_zip: str,
    output_dir: Path,
    forcar: bool = False,
) -> pd.DataFrame:
    """
    Baixa o ZIP nacional do Censo, extrai o CSV e filtra pelos setores
    do município indicado.

    O filtro usa os primeiros 7 dígitos da coluna 'Cod_setor', que
    correspondem ao código IBGE do município (CD_MUN).

    Parâmetros
    ----------
    codigo_ibge : str
        Código IBGE de 7 dígitos do município.
    url_base : str
        URL do diretório FTP onde o ZIP está localizado.
    prefixo_zip : str
        Prefixo do nome do ZIP no FTP. Ex:
        "Agregados_por_setores_caracteristicas_domicilio1_BR"
    output_dir : Path
        Pasta onde o ZIP será salvo (idempotência).
    forcar : bool
        Se True, rebaixa mesmo que o arquivo já exista.

    Retorna
    -------
    pd.DataFrame
        Linhas do CSV filtradas para o município, com Cod_setor como str.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Idempotência: verifica se o ZIP já foi baixado
    existentes = list(output_dir.glob(f"{prefixo_zip}*.zip"))
    if existentes and not forcar:
        zip_path = existentes[0]
        logger.info("ZIP ja existe (pulando download): %s", zip_path.name)
    else:
        url_zip = buscar_zip_no_ftp(url_base, prefixo=prefixo_zip)
        nome_zip = url_zip.split("/")[-1]
        zip_path = baixar_arquivo(url_zip, output_dir / nome_zip, forcar=forcar)

    # Lê o CSV de dentro do ZIP sem extrair para disco
    with zipfile.ZipFile(zip_path) as z:
        csvs = [n for n in z.namelist() if n.lower().endswith(".csv")]
        if not csvs:
            raise FileNotFoundError(f"Nenhum CSV encontrado em {zip_path.name}")
        csv_name = csvs[0]
        logger.info("Lendo %s de %s", csv_name, zip_path.name)

        with z.open(csv_name) as f:
            df = pd.read_csv(
                io.TextIOWrapper(f, encoding="latin-1"),
                sep=";",
                dtype=str,       # mantém zeros à esquerda em Cod_setor
                low_memory=False,
            )

    # Normaliza nome da coluna de setor — varia entre arquivos:
    # domicilio1 usa 'Cod_setor', domicilio2 usa 'setor'
    col_setor = next(
        (c for c in df.columns if c.lower() in ("cod_setor", "cd_setor", "setor")),
        None,
    )
    if col_setor is None:
        raise KeyError(
            f"Coluna de setor não encontrada. Colunas disponíveis: {list(df.columns[:10])}"
        )
    if col_setor != "Cod_setor":
        df = df.rename(columns={col_setor: "Cod_setor"})

    # Filtro por município: primeiros 7 dígitos de Cod_setor
    df_mun = df[df["Cod_setor"].str[:7] == codigo_ibge].copy()
    logger.info(
        "Filtro municipio %s: %d/%d setores retidos",
        codigo_ibge, len(df_mun), len(df),
    )
    return df_mun


def coletar_grupo2(
    codigo_ibge: str,
    limite_municipal: gpd.GeoDataFrame,
    output_dir: Path,
    db_conn,
    forcar: bool = False,
    **kwargs,
) -> dict:
    """
    Coleta dados tabulares do Censo 2022 para o município.

    Parâmetros
    ----------
    codigo_ibge : str
        Código IBGE de 7 dígitos. Ex: "2701407".
    limite_municipal : gpd.GeoDataFrame
        Não utilizado diretamente neste grupo (filtro é por Cod_setor).
    output_dir : Path
        Pasta de saída: data/raw/{uf}_{municipio}/censo/
    db_conn : duckdb.DuckDBPyConnection
        Conexão aberta com o DuckDB do município.
    forcar : bool
        Se True, rebaixa arquivos mesmo que já existam.

    Retorna
    -------
    dict
        {"status": "ok"|"erro", "camadas": [...], "mensagem": "..."}
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    camadas_salvas = []

    try:
        for nome_tabela, (url_base, prefixo) in _ARQUIVOS_CENSO.items():
            logger.info("[Grupo 2] Baixando %s", nome_tabela)
            df = _baixar_e_filtrar_csv(codigo_ibge, url_base, prefixo, output_dir, forcar=forcar)

            # Salva CSV filtrado em disco
            csv_saida = output_dir / f"{nome_tabela}.csv"
            df.to_csv(csv_saida, index=False, encoding="utf-8")
            logger.info("[Grupo 2] CSV salvo: %s (%d linhas)", csv_saida.name, len(df))

            # Persiste no DuckDB
            salvar_dataframe(db_conn, df, nome_tabela)
            camadas_salvas.append(nome_tabela)

        return {
            "status": "ok",
            "camadas": camadas_salvas,
            "mensagem": f"{len(camadas_salvas)} tabelas coletadas.",
        }

    except Exception as exc:
        logger.error("[Grupo 2] Erro: %s", exc, exc_info=True)
        return {
            "status": "erro",
            "camadas": camadas_salvas,
            "mensagem": str(exc),
        }
