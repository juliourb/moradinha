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

def _gerar_mapa(db_conn: duckdb.DuckDBPyConnection, nome_municipio: str, output_path: Path) -> None:
    """
    Plota as camadas espaciais coletadas e salva em PNG.

    Camadas e estilos:
        setores_censitarios  — preenchimento cinza claro, borda cinza
        areas_ponderacao     — sem preenchimento, borda azul tracejada
        grade_estatistica    — sem preenchimento, borda cinza médio
        faces_logradouro     — linhas amarelo escuro
        eixos_osm            — linhas amarelo claro
        limite_municipal     — sem preenchimento, borda preta espessa
    """
    tabelas = listar_tabelas(db_conn)

    fig, ax = plt.subplots(figsize=(12, 12))
    ax.set_aspect("equal")
    ax.set_axis_off()
    ax.set_facecolor("#1a1a2e")
    fig.patch.set_facecolor("#1a1a2e")

    legenda = []

    # Ordem de plotagem: do fundo para frente
    _camadas = [
        ("setores_censitarios", dict(color="#2e2e3e", edgecolor="#888888", linewidth=0.4, alpha=0.9),  "#2e2e3e", "Setores censitários"),
        ("areas_ponderacao",    dict(color="none",    edgecolor="#5b9bd5", linewidth=1.0, linestyle="--"), "#5b9bd5", "Áreas de ponderação"),
        ("grade_estatistica",   dict(color="none",    edgecolor="#666688", linewidth=0.3),               "#666688", "Grade estatística 200m"),
        ("faces_logradouro",    dict(color="#b8860b",                       linewidth=0.8),               "#b8860b", "Faces de logradouro (IBGE)"),
        ("eixos_osm",           dict(color="#ffe57a",                       linewidth=0.6),               "#ffe57a", "Eixos viários (OSM)"),
        ("limite_municipal",    dict(color="none",    edgecolor="#ffffff",  linewidth=1.8),               "#ffffff", "Limite municipal"),
    ]

    for nome_tabela, estilo, cor_legenda, rotulo in _camadas:
        if nome_tabela not in tabelas:
            continue
        try:
            gdf = ler_tabela_espacial(db_conn, nome_tabela)
            if gdf.empty:
                continue
            gdf.plot(ax=ax, **estilo)
            patch = mpatches.Patch(color=cor_legenda, label=rotulo)
            legenda.append(patch)
        except Exception as exc:
            logger.warning("Não foi possível plotar '%s': %s", nome_tabela, exc)

    if legenda:
        ax.legend(
            handles=legenda,
            loc="lower left",
            fontsize=8,
            framealpha=0.6,
            facecolor="#1a1a2e",
            labelcolor="white",
            edgecolor="#444444",
        )

    titulo = nome_municipio.replace("_", " ").title()
    ax.set_title(f"{titulo} — dados coletados", color="white", fontsize=13, pad=12)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=150, bbox_inches="tight", facecolor=fig.get_facecolor())
    plt.close(fig)
    logger.info("Mapa salvo em: %s", output_path)


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
            _gerar_mapa(db_conn, nome_municipio, mapa_path)
            resultados["mapa"] = str(mapa_path)
        except Exception as exc:
            logger.error("Erro ao gerar mapa: %s", exc, exc_info=True)
            resultados["mapa"] = None

    finally:
        db_conn.close()

    # --- Resumo ---
    logger.info("=" * 60)
    logger.info("Coleta concluída para %s", nome_municipio)
    for g, res in resultados.items():
        if g == "mapa":
            continue
        status = res.get("status", "?")
        camadas = res.get("camadas", [])
        logger.info("  Grupo %d: %s | camadas: %s", g, status, camadas)
    if resultados.get("mapa"):
        logger.info("  Mapa: %s", resultados["mapa"])

    return resultados
