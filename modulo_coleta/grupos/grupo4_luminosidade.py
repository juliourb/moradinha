"""
grupo4_luminosidade.py — Coleta de dados de luminosidade noturna VIIRS VNL V2.2.

Fonte: Earth Observation Group (EOG) — Colorado School of Mines
URL base: https://eogdata.mines.edu/nighttime_light/annual/v22/{ano}/
Arquivo: *_average_masked.tif
Licença: CC BY 4.0 — REQUER TOKEN DE AUTENTICAÇÃO EOG (registro gratuito)

Modos de operação (parâmetro `modo`)
--------------------------------------
tile_local (padrão)
    Usa um tile .tif já baixado manualmente. Exige `tile_path`.
    Indicado quando o tile global (~500 MB) já está disponível em disco.

download (futuro — não implementado)
    Baixa o tile diretamente da EOG com autenticação via token.
    Exige `eog_token`.

Fluxo (modo tile_local)
-----------------------
1. Clip do tile pelo limite municipal → salva GeoTIFF recortado
2. Zonal stats sobre setores censitários → tabela luminosidade_{ano}
3. Zonal stats sobre grade estatística 200m (opcional, se tabela existir)

Arquivos gerados em data/raw/{municipio}/luminosidade/
    viirs_{ano}_recortado.tif

Tabelas DuckDB geradas
    luminosidade_{ano}           — stats por setor censitário
    luminosidade_{ano}_grade200  — stats por célula de grade 200m (se disponível)

Dependências: utils/raster_utils.py, utils/db_utils.py
"""

from __future__ import annotations

import logging
import re
from pathlib import Path

import duckdb
import geopandas as gpd

from ..utils.db_utils import salvar_dataframe
from ..utils.raster_utils import clip_raster, zonal_stats_por_camada, ler_tabela_espacial

logger = logging.getLogger(__name__)

# Estatísticas calculadas para cada zona
_STATS_PADRAO = ["mean", "median", "max", "std", "count"]


def _inferir_ano_do_tile(tile_path: Path) -> int | None:
    """
    Tenta extrair o ano do nome do arquivo tile.

    Ex: VNL_v22_npp-j01_2022_global_... → 2022
    """
    match = re.search(r"_(20\d{2})_", tile_path.stem)
    if match:
        return int(match.group(1))
    return None


def coletar_grupo4(
    codigo_ibge: str,
    limite_municipal: gpd.GeoDataFrame,
    output_dir: Path,
    db_conn: duckdb.DuckDBPyConnection,
    modo: str = "tile_local",
    tile_path: Path | str | None = None,
    ano: int | None = None,
    stats: list[str] | None = None,
    forcar: bool = False,
    eog_token: str | None = None,
    **kwargs,
) -> dict:
    """
    Coleta e processa dados de luminosidade noturna VIIRS para o município.

    Parâmetros
    ----------
    codigo_ibge : str
        Código IBGE de 7 dígitos. Ex: "3524402".
    limite_municipal : gpd.GeoDataFrame
        GeoDataFrame do limite municipal (usado como geometria de clip).
    output_dir : Path
        Pasta de saída: data/raw/{uf}_{municipio}/luminosidade/
    db_conn : duckdb.DuckDBPyConnection
        Conexão aberta com o DuckDB do município.
        Deve conter a tabela 'setores_censitarios' (criada pelo grupo1).
    modo : str
        'tile_local' (padrão) — usa tile .tif já baixado em `tile_path`.
        'download' — não implementado (requer token EOG).
    tile_path : Path | str | None
        Caminho para o arquivo .tif (obrigatório quando modo='tile_local').
    ano : int | None
        Ano do produto VIIRS. Se None, inferido automaticamente do nome do arquivo.
        Ex: 2022, 2024.
    stats : list[str] | None
        Estatísticas zonais a calcular.
        Padrão: ["mean", "median", "max", "std", "count"].
    forcar : bool
        Se True, recalcula mesmo que o TIF recortado já exista.
    eog_token : str | None
        Token EOG (reservado para modo 'download', ignorado em 'tile_local').

    Retorna
    -------
    dict
        {"status": "ok"|"erro", "camadas": [...], "mensagem": "..."}

    Levanta
    -------
    NotImplementedError
        Se modo='download' for solicitado.
    ValueError
        Se modo='tile_local' for usado sem `tile_path`.

    Notas
    -----
    - O clip usa a geometria do limite municipal reprojetada para o CRS do tile.
    - As zonal stats são calculadas sobre os setores censitários presentes no DuckDB.
    - Se a tabela 'grade_estatistica' existir no DuckDB, as stats são calculadas
      também para a grade 200m (tabela luminosidade_{ano}_grade200).
    - Pixels com valor nodata (definido nos metadados do TIF) são ignorados.
    """
    if modo == "download":
        raise NotImplementedError(
            "modo='download' não implementado. "
            "Baixe o tile manualmente em https://eogdata.mines.edu/nighttime_light/ "
            "e use modo='tile_local' com tile_path apontando para o arquivo."
        )

    if modo != "tile_local":
        raise ValueError(f"modo='{modo}' desconhecido. Use 'tile_local'.")

    if tile_path is None:
        raise ValueError(
            "tile_path é obrigatório quando modo='tile_local'. "
            "Informe o caminho para o arquivo .tif do tile VIIRS."
        )

    tile_path = Path(tile_path)
    if not tile_path.exists():
        raise FileNotFoundError(f"Tile não encontrado: {tile_path}")

    # Inferir ano se não fornecido
    if ano is None:
        ano = _inferir_ano_do_tile(tile_path)
        if ano is None:
            raise ValueError(
                "Não foi possível inferir o ano do nome do arquivo tile. "
                f"Informe o parâmetro `ano` explicitamente. Arquivo: {tile_path.name}"
            )
        logger.info("Ano inferido do nome do tile: %d", ano)

    if stats is None:
        stats = _STATS_PADRAO

    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    camadas_salvas = []

    try:
        # -----------------------------------------------------------------
        # 1. Clip do tile pelo limite municipal
        # -----------------------------------------------------------------
        tif_recortado = output_dir / f"viirs_{ano}_recortado.tif"

        if tif_recortado.exists() and not forcar:
            logger.info(
                "TIF recortado já existe (%s) — pulando clip. Use forcar=True para reclacular.",
                tif_recortado.name,
            )
        else:
            logger.info("[Grupo 4] Recortando tile VIIRS %d pelo limite municipal...", ano)
            clip_raster(tile_path, limite_municipal, tif_recortado)

        # -----------------------------------------------------------------
        # 2. Zonal stats sobre setores censitários
        # -----------------------------------------------------------------
        logger.info("[Grupo 4] Calculando zonal stats por setor censitário...")

        tabelas_db = db_conn.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
        ).fetchdf()["table_name"].tolist()

        if "setores_censitarios" not in tabelas_db:
            raise RuntimeError(
                "Tabela 'setores_censitarios' não encontrada no DuckDB. "
                "Execute o grupo1 antes do grupo4."
            )

        gdf_setores = ler_tabela_espacial(db_conn, "setores_censitarios")

        df_stats_setores = zonal_stats_por_camada(
            tif_recortado, gdf_setores, stats=stats, prefixo="viirs"
        )

        # Juntar código do setor com as estatísticas
        col_setor = next(
            (c for c in gdf_setores.columns if c.upper() in ("CD_SETOR", "COD_SETOR")),
            None,
        )
        if col_setor:
            df_stats_setores.insert(0, "CD_SETOR", gdf_setores[col_setor].values)

        df_stats_setores["ano"] = ano

        nome_tabela_setores = f"luminosidade_{ano}"
        salvar_dataframe(db_conn, df_stats_setores, nome_tabela_setores)
        camadas_salvas.append(nome_tabela_setores)
        logger.info(
            "[Grupo 4] Tabela '%s' salva: %d setores.",
            nome_tabela_setores,
            len(df_stats_setores),
        )

        # -----------------------------------------------------------------
        # 3. Zonal stats sobre grade estatística 200m (opcional)
        # -----------------------------------------------------------------
        if "grade_estatistica" in tabelas_db:
            logger.info("[Grupo 4] Calculando zonal stats por grade 200m...")
            gdf_grade = ler_tabela_espacial(db_conn, "grade_estatistica")

            df_stats_grade = zonal_stats_por_camada(
                tif_recortado, gdf_grade, stats=stats, prefixo="viirs"
            )

            # Adicionar ID da célula de grade para permitir joins
            col_id_grade = next(
                (c for c in gdf_grade.columns if c.upper() in ("ID_UNICO", "ID", "CELL_ID")),
                None,
            )
            if col_id_grade:
                df_stats_grade.insert(0, col_id_grade, gdf_grade[col_id_grade].values)

            df_stats_grade["ano"] = ano

            nome_tabela_grade = f"luminosidade_{ano}_grade200"
            salvar_dataframe(db_conn, df_stats_grade, nome_tabela_grade)
            camadas_salvas.append(nome_tabela_grade)
            logger.info(
                "[Grupo 4] Tabela '%s' salva: %d células.",
                nome_tabela_grade,
                len(df_stats_grade),
            )
        else:
            logger.info(
                "[Grupo 4] Tabela 'grade_estatistica' não encontrada no DuckDB — "
                "zonal stats por grade 200m pulados."
            )

        return {
            "status": "ok",
            "camadas": camadas_salvas,
            "mensagem": (
                f"Luminosidade {ano} processada: {len(camadas_salvas)} tabela(s). "
                f"TIF recortado: {tif_recortado.name}"
            ),
        }

    except Exception as exc:
        logger.error("[Grupo 4] Erro: %s", exc, exc_info=True)
        return {
            "status": "erro",
            "camadas": camadas_salvas,
            "mensagem": str(exc),
        }
