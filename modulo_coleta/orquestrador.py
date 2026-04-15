"""
orquestrador.py — Ponto de entrada do módulo de coleta.

Função principal:
    coletar_municipio(codigo_ibge, grupos, base_dir, nome_municipio, forcar)

Uso:
    from modulo_coleta.orquestrador import coletar_municipio

    # Coleta grupos 1-3 de Campo Alegre-AL
    coletar_municipio("2701407", grupos=[1, 2, 3], nome_municipio="al_campo_alegre")

    # Coleta completa (grupo 4 requer tile VIIRS em data/raw/tiles_globais/)
    coletar_municipio("2704302", grupos=[1, 2, 3, 4, 5], nome_municipio="al_maceio")

Grupo 4 — luminosidade noturna:
    O tile VIIRS global (~11 GB) deve ser baixado manualmente e salvo em:
        {base_dir}/raw/tiles_globais/
    Arquivo esperado: *average_masked*.tif
    Download em: https://eogdata.mines.edu/nighttime_light/annual/v22/
    (Requer registro gratuito — licença CC BY 4.0)
"""

from __future__ import annotations

import logging
import unicodedata
from pathlib import Path

import duckdb
import geopandas as gpd
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches

from .grupos.grupo1_geometrias import coletar_grupo1
from .grupos.grupo2_censo import coletar_grupo2
from .grupos.grupo3_logradouros import coletar_grupo3
from .grupos.grupo4_luminosidade import coletar_grupo4
from .grupos.grupo5_pnadc import coletar_grupo5
from .utils.db_utils import abrir_conexao, listar_tabelas
from .utils.raster_utils import ler_tabela_espacial

logger = logging.getLogger(__name__)

GRUPOS_DISPONIVEIS: dict[int, str] = {
    1: "grupo1_geometrias",
    2: "grupo2_censo",
    3: "grupo3_logradouros",
    4: "grupo4_luminosidade",
    5: "grupo5_pnadc",
}

# Subpastas de dados brutos por grupo
_SUBDIR_GRUPO: dict[int, str] = {
    1: "geometria",
    2: "censo",
    3: "logradouros",
    4: "luminosidade",
    5: "pnadc",
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _derivar_nome_municipio(codigo_ibge: str) -> str:
    """
    Deriva o nome canônico da pasta a partir do código IBGE via geobr.

    Ex: "2701407" → "al_campo_alegre"
    """
    import geobr
    logger.info("Derivando nome do município via geobr (codigo_ibge=%s)...", codigo_ibge)
    gdf = geobr.read_municipality(code_muni=int(codigo_ibge), year=2022)
    sigla_uf = gdf["abbrev_state"].iloc[0].lower()
    nome = gdf["name_muni"].iloc[0].lower()
    # Remove acentos
    nome = unicodedata.normalize("NFD", nome)
    nome = "".join(c for c in nome if unicodedata.category(c) != "Mn")
    nome = nome.replace(" ", "_").replace("-", "_").replace("'", "")
    return f"{sigla_uf}_{nome}"


def _buscar_tile_viirs(tiles_dir: Path) -> Path | None:
    """
    Procura o tile VIIRS average_masked na pasta tiles_globais.

    Retorna o Path do arquivo se encontrado, None caso contrário.
    """
    candidatos = list(tiles_dir.glob("*average_masked*.tif"))
    if not candidatos:
        return None
    if len(candidatos) > 1:
        logger.warning(
            "Mais de um tile VIIRS encontrado em %s — usando o mais recente: %s",
            tiles_dir,
            candidatos[-1].name,
        )
    return candidatos[-1]


# ---------------------------------------------------------------------------
# Mapa de coleta
# ---------------------------------------------------------------------------

def _ler_camada(db_conn: duckdb.DuckDBPyConnection, tabelas: list[str], nome: str) -> gpd.GeoDataFrame | None:
    """Lê uma camada espacial do DuckDB. Retorna None se não existir ou estiver vazia."""
    if nome not in tabelas:
        return None
    try:
        gdf = ler_tabela_espacial(db_conn, nome)
        return gdf if not gdf.empty else None
    except Exception as exc:
        logger.warning("Não foi possível ler '%s': %s", nome, exc)
        return None


def _buscar_tabela_luminosidade(tabelas: list[str]) -> str | None:
    """Retorna o nome da tabela de luminosidade por setor (ex: 'luminosidade_2022')."""
    import re
    candidatas = [t for t in tabelas if re.fullmatch(r"luminosidade_\d{4}", t)]
    return candidatas[-1] if candidatas else None


def _buscar_tabela_luminosidade_grade200(tabelas: list[str]) -> str | None:
    """Retorna o nome da tabela de luminosidade por grade 200m (ex: 'luminosidade_2022_grade200')."""
    import re
    candidatas = [t for t in tabelas if re.fullmatch(r"luminosidade_\d{4}_grade200", t)]
    return candidatas[-1] if candidatas else None


def _buscar_raster_viirs(raw_dir: Path) -> Path | None:
    """Procura o TIF VIIRS recortado em raw_dir/luminosidade/. Retorna None se não encontrar."""
    lum_dir = raw_dir / "luminosidade"
    if not lum_dir.exists():
        return None
    candidatos = sorted(lum_dir.glob("viirs_*_recortado.tif"))
    return candidatos[-1] if candidatos else None


def _gerar_mapa(db_conn: duckdb.DuckDBPyConnection, nome_municipio: str, output_path: Path, codigo_ibge: str) -> plt.Figure:
    """
    Gera mapa vetorial com luminosidade como fundo e camadas sobrepostas.

    Sempre gera o mapa vetorial com:
    - Luminosidade agregada na grade 200m como fundo (alpha=0.4, colormap YlOrRd, normalizado 5-95%)
    - Camadas vetoriais sobrepostas na ordem especificada
    - Legenda com patches e colorbar para luminosidade
    """
    import re
    import numpy as np
    import matplotlib.colors as mcolors
    from matplotlib.lines import Line2D

    tabelas = listar_tabelas(db_conn)
    titulo = f'Camadas coletadas — {nome_municipio.replace("_", " ").title()} ({codigo_ibge})'

    fig, ax = plt.subplots(figsize=(14, 12))
    fig.patch.set_facecolor("#f7f7f7")
    ax.set_facecolor("#e8edf2")
    ax.set_aspect("equal")
    ax.set_axis_off()

    handles = []

    # ------------------------------------------------------------------
    # 1. Fundo: Luminosidade na grade estatística (alpha=0.4)
    # ------------------------------------------------------------------
    gdf_grade = _ler_camada(db_conn, tabelas, "grade_estatistica")
    tabela_lum_grade = _buscar_tabela_luminosidade_grade200(tabelas)
    df_lum = None
    if tabela_lum_grade is not None:
        try:
            df_lum = db_conn.execute(f"SELECT * FROM {tabela_lum_grade}").fetchdf()
        except Exception as exc:
            logger.warning("Não foi possível ler '%s': %s", tabela_lum_grade, exc)

    if gdf_grade is not None and df_lum is not None:
        # Encontrar coluna de ID para merge
        col_id = next(
            (c for c in gdf_grade.columns if c.upper() in ("ID_UNICO", "ID", "CELL_ID")),
            None,
        )
        if col_id and col_id in df_lum.columns:
            gdf_merged = gdf_grade.merge(df_lum, on=col_id, how="left")
            if "viirs_mean" in gdf_merged.columns:
                values = gdf_merged["viirs_mean"]
                if values.notna().any():
                    vmin, vmax = np.percentile(values.dropna(), [5, 95])
                    gdf_merged.plot(
                        ax=ax,
                        column="viirs_mean",
                        cmap="YlOrRd",
                        alpha=0.4,
                        vmin=vmin,
                        vmax=vmax,
                        legend=False,
                    )
                    # Colorbar
                    sm = plt.cm.ScalarMappable(
                        cmap="YlOrRd",
                        norm=mcolors.Normalize(vmin=vmin, vmax=vmax),
                    )
                    sm.set_array([])
                    cbar = plt.colorbar(sm, ax=ax, shrink=0.5, pad=0.02, aspect=30)
                    cbar.set_label("Luminosidade média (nW/cm²/sr)", fontsize=10, labelpad=8)
                    cbar.ax.yaxis.set_tick_params(labelsize=8)
                    handles.append(mpatches.Patch(facecolor="#f9e2c5", edgecolor="#cccccc", label="Luminosidade (grade 200m)"))
                else:
                    logger.warning("Nenhum valor válido de luminosidade encontrado na grade.")
            else:
                logger.warning("Coluna 'viirs_mean' não encontrada em %s.", tabela_lum_grade)
        else:
            logger.warning("Coluna de ID não encontrada para merge da luminosidade com grade.")
    else:
        logger.info("Grade estatística ou luminosidade não disponíveis — fundo sem cor.")

    # ------------------------------------------------------------------
    # 2. Setores censitários (bordas apenas)
    # ------------------------------------------------------------------
    gdf = _ler_camada(db_conn, tabelas, "setores_censitarios")
    if gdf is not None:
        gdf.plot(ax=ax, color="none", edgecolor="#555555", linewidth=0.5)
        handles.append(mpatches.Patch(edgecolor="#555555", facecolor="none", linewidth=0.5, label="Setores censitários"))

    # ------------------------------------------------------------------
    # 3. Áreas de ponderação (bordas tracejadas)
    # ------------------------------------------------------------------
    gdf = _ler_camada(db_conn, tabelas, "areas_ponderacao")
    if gdf is not None:
        gdf.plot(ax=ax, color="none", edgecolor="blue", linewidth=1.2, linestyle="--")
        handles.append(Line2D([0], [0], color="blue", linewidth=1.2, linestyle="--", label="Áreas de ponderação"))

    # ------------------------------------------------------------------
    # 4. Faces de logradouro (linhas finas)
    # ------------------------------------------------------------------
    gdf = _ler_camada(db_conn, tabelas, "faces_logradouro")
    if gdf is not None:
        gdf.plot(ax=ax, color="#888888", linewidth=0.3, alpha=0.6)
        handles.append(Line2D([0], [0], color="#888888", linewidth=1.0, alpha=0.6, label="Faces de logradouro"))

    # ------------------------------------------------------------------
    # 5. Eixos OSM (linhas)
    # ------------------------------------------------------------------
    gdf = _ler_camada(db_conn, tabelas, "eixos_osm")
    if gdf is not None:
        gdf.plot(ax=ax, color="#333333", linewidth=0.5, alpha=0.7)
        handles.append(Line2D([0], [0], color="#333333", linewidth=1.0, alpha=0.7, label="Eixos viários (OSM)"))

    # ------------------------------------------------------------------
    # 6. Endereços CNEFE (pontos pequenos)
    # ------------------------------------------------------------------
    gdf = _ler_camada(db_conn, tabelas, "enderecos_cnefe")
    if gdf is not None:
        amostra = gdf.sample(min(2000, len(gdf)), random_state=42) if len(gdf) > 2000 else gdf
        amostra.plot(ax=ax, color="#222222", markersize=0.8, alpha=0.3)
        rotulo = f"Endereços CNEFE ({len(amostra):,})" if len(gdf) > 2000 else f"Endereços CNEFE ({len(gdf):,})"
        handles.append(Line2D([0], [0], marker="o", color="w", markerfacecolor="#222222", markersize=3, alpha=0.5, label=rotulo))

    # ------------------------------------------------------------------
    # 7. Limite municipal (borda externa grossa)
    # ------------------------------------------------------------------
    gdf = _ler_camada(db_conn, tabelas, "limite_municipal")
    if gdf is not None:
        gdf.plot(ax=ax, color="none", edgecolor="black", linewidth=2.0)
        handles.append(Line2D([0], [0], color="black", linewidth=2.0, label="Limite municipal"))

    # ------------------------------------------------------------------
    # Legenda
    # ------------------------------------------------------------------
    ax.legend(handles=handles, loc="lower left", fontsize=8, framealpha=0.9, edgecolor="#cccccc")

    fig.suptitle(titulo, fontsize=14, fontweight="bold", y=0.95)
    fig.tight_layout()

    # ------------------------------------------------------------------
    # Salvamento
    # ------------------------------------------------------------------
    fig.savefig(output_path, dpi=150, bbox_inches="tight", facecolor=fig.get_facecolor())
    logger.info("Mapa salvo em: %s", output_path)
    return fig


# ---------------------------------------------------------------------------
# Função principal
# ---------------------------------------------------------------------------

def coletar_municipio(
    codigo_ibge: str,
    grupos: list[int] = [1, 2, 3, 4, 5],
    base_dir: Path = Path("data"),
    nome_municipio: str | None = None,
    forcar: bool = False,
) -> dict:
    """
    Coleta todos os dados necessários para um município e persiste em DuckDB.

    Parâmetros
    ----------
    codigo_ibge : str
        Código IBGE do município com 7 dígitos. Ex: "2701407" (Campo Alegre-AL).
    grupos : list[int]
        Quais grupos coletar. Padrão: [1, 2, 3, 4, 5].
        1=Geometrias, 2=Censo, 3=Logradouros, 4=Luminosidade, 5=PNADc.
    base_dir : Path
        Pasta raiz dos dados. Padrão: "data/" relativa ao diretório de trabalho.
    nome_municipio : str | None
        Nome canônico da pasta. Ex: "al_campo_alegre".
        Se None, derivado automaticamente via geobr.
    forcar : bool
        Se True, reprocessa mesmo que os arquivos já existam.

    Retorna
    -------
    dict
        Resultado por grupo: {1: {"status": "ok", ...}, 2: {...}, ...}
        Inclui chave "mapa" com o caminho do PNG gerado.
    """
    base_dir = Path(base_dir)
    grupos_invalidos = [g for g in grupos if g not in GRUPOS_DISPONIVEIS]
    if grupos_invalidos:
        raise ValueError(f"Grupos inválidos: {grupos_invalidos}. Disponíveis: {list(GRUPOS_DISPONIVEIS)}")

    # --- Nome e caminhos ---
    if nome_municipio is None:
        nome_municipio = _derivar_nome_municipio(codigo_ibge)
    logger.info("Município: %s (IBGE: %s)", nome_municipio, codigo_ibge)

    raw_dir = base_dir / "raw" / nome_municipio
    processed_dir = base_dir / "processed" / nome_municipio
    db_path = processed_dir / f"{nome_municipio}.duckdb"

    raw_dir.mkdir(parents=True, exist_ok=True)
    processed_dir.mkdir(parents=True, exist_ok=True)

    resultados: dict = {}
    db_conn = abrir_conexao(db_path)

    try:
        limite_municipal: gpd.GeoDataFrame | None = None

        for grupo in sorted(grupos):
            nome_grupo = GRUPOS_DISPONIVEIS[grupo]
            output_dir = raw_dir / _SUBDIR_GRUPO[grupo]
            logger.info("=" * 60)
            logger.info("Iniciando %s...", nome_grupo)

            try:
                if grupo == 1:
                    resultado = coletar_grupo1(
                        codigo_ibge=codigo_ibge,
                        limite_municipal=None,
                        output_dir=output_dir,
                        db_conn=db_conn,
                        forcar=forcar,
                    )
                    # Lê o limite_municipal do banco para repassar aos grupos seguintes
                    if resultado["status"] == "ok":
                        limite_municipal = ler_tabela_espacial(db_conn, "limite_municipal")

                elif grupo == 2:
                    resultado = coletar_grupo2(
                        codigo_ibge=codigo_ibge,
                        limite_municipal=limite_municipal,
                        output_dir=output_dir,
                        db_conn=db_conn,
                        forcar=forcar,
                    )

                elif grupo == 3:
                    resultado = coletar_grupo3(
                        codigo_ibge=codigo_ibge,
                        limite_municipal=limite_municipal,
                        output_dir=output_dir,
                        db_conn=db_conn,
                        forcar=forcar,
                    )

                elif grupo == 4:
                    tiles_dir = base_dir / "raw" / "tiles_globais"
                    tile_path = _buscar_tile_viirs(tiles_dir)

                    if tile_path is None:
                        msg = (
                            "Tile VIIRS average_masked não encontrado em "
                            f"'{tiles_dir}'. "
                            "Baixe em: https://eogdata.mines.edu/nighttime_light/annual/v22/ "
                            "(registro gratuito, licença CC BY 4.0) e salve nessa pasta."
                        )
                        logger.error(msg)
                        resultado = {"status": "pulado", "camadas": [], "mensagem": msg}
                    else:
                        resultado = coletar_grupo4(
                            codigo_ibge=codigo_ibge,
                            limite_municipal=limite_municipal,
                            output_dir=output_dir,
                            db_conn=db_conn,
                            modo="tile_local",
                            tile_path=tile_path,
                            forcar=forcar,
                        )

                elif grupo == 5:
                    resultado = coletar_grupo5(
                        codigo_ibge=codigo_ibge,
                        limite_municipal=limite_municipal,
                        output_dir=output_dir,
                        db_conn=db_conn,
                        forcar=forcar,
                    )

            except Exception as exc:
                logger.error("[Grupo %d] Erro inesperado: %s", grupo, exc, exc_info=True)
                resultado = {"status": "erro", "camadas": [], "mensagem": str(exc)}

            resultados[grupo] = resultado
            status = resultado.get("status", "?")
            logger.info("Grupo %d finalizado — status: %s", grupo, status)

        # --- Mapa ---
        logger.info("=" * 60)
        logger.info("Gerando mapa de coleta...")
        mapa_path = processed_dir / "mapa_coleta.png"
        try:
            fig = _gerar_mapa(db_conn, nome_municipio, mapa_path, codigo_ibge)
            resultados["mapa"] = str(mapa_path)
            resultados["fig"] = fig
        except Exception as exc:
            logger.error("Erro ao gerar mapa: %s", exc, exc_info=True)
            resultados["mapa"] = None
            resultados["fig"] = None

    finally:
        db_conn.close()

    # --- Resumo ---
    logger.info("=" * 60)
    logger.info("Coleta concluída para %s", nome_municipio)
    for g, res in resultados.items():
        if g in ("mapa", "fig"):
            continue
        status = res.get("status", "?")
        camadas = res.get("camadas", [])
        logger.info("  Grupo %d: %s | camadas: %s", g, status, camadas)
    if resultados.get("mapa"):
        logger.info("  Mapa: %s", resultados["mapa"])

    return resultados
