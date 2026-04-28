"""
grupo5_pnadc.py — Coleta de dados da PNADc em Python puro (sem dependência R).

⚠️ LIMITAÇÃO FUNDAMENTAL: A PNADc NÃO tem representatividade municipal.
   A menor unidade geográfica é a ÁREA DE PONDERAÇÃO (identificada por V1029).
   Os resultados são válidos para o domínio de estimação V1029, não para o município.

Fluxo: FTP IBGE (microdados + dicionário) → pandas → samplics → DuckDB

Reescrito de: grupo5_pnadc.R_backup.py (dependia de subprocess + R)
Motivo: Smart App Control (SAC) do Windows 11 bloqueava DLLs do R.

Tabelas DuckDB geradas:
    pnadc_estimativas  — média ponderada por variável (estimativa, SE, IC, n_obs)
    pnadc_metadados    — domínio V1029 usado, variáveis ausentes, avisos

Dependências: samplics, openpyxl ou xlrd (para dicionário .xls/.xlsx), requests
"""

from __future__ import annotations

import logging
import re
import time
import zipfile
from pathlib import Path

import geobr
import geopandas as gpd
import pandas as pd
import requests

from ..utils.db_utils import salvar_dataframe

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constantes
# ---------------------------------------------------------------------------

_BASE_FTP = (
    "https://ftp.ibge.gov.br/Trabalho_e_Rendimento/"
    "Pesquisa_Nacional_por_Amostra_de_Domicilios_continua/"
    "Trimestral/Microdados"
)

VARS_DESIGN = ["Ano", "Trimestre", "UF", "Estrato", "UPA", "V1028"]
VARS_GEO    = ["V1029", "V1022", "V1023"]
VARS_HAB    = [
    "V2001", "VD5008",
    "S01007A", "S01011C", "S01012A", "S01013", "S01017", "S01019",
]
VARS_NECESSARIAS = VARS_DESIGN + VARS_GEO + VARS_HAB

# ---------------------------------------------------------------------------
# Etapa 3a — Download de microdados e dicionário
# ---------------------------------------------------------------------------

def _listar_ftp_ibge(url_dir: str) -> list[str]:
    """Lists file names from an IBGE HTTPS-FTP directory (Apache index)."""
    url = url_dir.rstrip("/") + "/"
    resp = requests.get(url, timeout=60)
    resp.raise_for_status()
    # Apache directory listing: <a href="filename">filename</a>
    nomes = re.findall(r'href="([^"/?][^"]*)"', resp.text)
    return nomes


def _download_arquivo(url: str, dest: Path) -> None:
    """Downloads url → dest in streaming mode, logging URL, size and elapsed time."""
    t0 = time.time()
    resp = requests.get(url, stream=True, timeout=600)
    resp.raise_for_status()

    total = int(resp.headers.get("content-length", 0))
    baixado = 0
    chunk_mb = 4 * 1024 * 1024  # 4 MB chunks

    dest.parent.mkdir(parents=True, exist_ok=True)
    with open(dest, "wb") as f:
        for chunk in resp.iter_content(chunk_size=chunk_mb):
            if chunk:
                f.write(chunk)
                baixado += len(chunk)
                if total:
                    logger.info(
                        "[Grupo 5] %.0f%% — %.1f / %.1f MB",
                        100 * baixado / total, baixado / 1e6, total / 1e6,
                    )

    elapsed = time.time() - t0
    logger.info(
        "[Grupo 5] Download concluído: %s — %.1f MB em %.0fs",
        dest.name, baixado / 1e6, elapsed,
    )


def _baixar_microdados_pnadc(ano: int, trimestre: int, output_dir: Path) -> tuple[Path, Path]:
    """
    Downloads PNADc quarterly microdata and data dictionary from IBGE FTP.

    Parameters
    ----------
    ano : int
        Reference year (e.g., 2022).
    trimestre : int
        Quarter 1–4.
    output_dir : Path
        Directory for caching downloaded files. Skips download if files
        already exist (cache check by filename).

    Returns
    -------
    (zip_path, dict_path) : tuple[Path, Path]
        zip_path  — microdata ZIP  (e.g. PNADC_042022.zip, ~200 MB)
        dict_path — XLS/XLSX data dictionary (column positions and types)

    Notes
    -----
    - First run downloads ~200 MB per quarter (5–15 min depending on connection).
    - The microdata file uses a fixed-width text format; the dictionary provides
      the column positions needed to parse it correctly.
    - Both files are cached in output_dir: subsequent calls return immediately.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # ── Microdata ZIP ──────────────────────────────────────────────────────
    url_ano = f"{_BASE_FTP}/{ano}/"
    logger.info("[Grupo 5] Listando diretório FTP: %s", url_ano)
    arquivos_ano = _listar_ftp_ibge(url_ano)
    logger.info("[Grupo 5] Arquivos disponíveis (%d): %s", len(arquivos_ano), arquivos_ano)

    # IBGE naming: PNADC_{trimestre:02d}{ano}.zip  (e.g. PNADC_042022.zip)
    padrao_dados = f"PNADC_{trimestre:02d}{ano}"
    arquivo_dados = next(
        (a for a in arquivos_ano if padrao_dados in a and a.lower().endswith(".zip")),
        None,
    )
    if arquivo_dados is None:
        raise FileNotFoundError(
            f"Arquivo PNADc não encontrado para {ano}T{trimestre} em {url_ano}.\n"
            f"Padrão buscado: '{padrao_dados}*.zip'\n"
            f"Arquivos disponíveis: {arquivos_ano}"
        )

    zip_path = output_dir / arquivo_dados
    if zip_path.exists():
        logger.info(
            "[Grupo 5] Cache hit (microdados): %s — %.1f MB",
            zip_path.name, zip_path.stat().st_size / 1e6,
        )
    else:
        url_dados = f"{url_ano}{arquivo_dados}"
        logger.info(
            "[Grupo 5] Baixando microdados PNADc %dT%d: %s "
            "(pode demorar 5–15 min na 1ª execução)",
            ano, trimestre, url_dados,
        )
        _download_arquivo(url_dados, zip_path)

    # ── Dicionário (posições das colunas) ──────────────────────────────────
    url_doc = f"{_BASE_FTP}/Documentacao/"
    logger.info("[Grupo 5] Listando documentação: %s", url_doc)
    arquivos_doc = _listar_ftp_ibge(url_doc)
    logger.info("[Grupo 5] Documentação disponível (%d): %s", len(arquivos_doc), arquivos_doc)

    def _eh_xls(nome: str) -> bool:
        return nome.lower().endswith((".xls", ".xlsx"))

    def _eh_zip_dicionario(nome: str) -> bool:
        return "dicionario" in nome.lower() and nome.lower().endswith(".zip")

    # Prioridade 1: .xls/.xlsx com "dicionario" no nome, direto na pasta
    nome_dict_direto = next(
        (a for a in arquivos_doc if "dicionario" in a.lower() and _eh_xls(a)),
        None,
    )

    if nome_dict_direto is not None:
        dict_path = output_dir / nome_dict_direto
        if dict_path.exists():
            logger.info("[Grupo 5] Cache hit (dicionário): %s", dict_path.name)
        else:
            _download_arquivo(f"{url_doc}{nome_dict_direto}", dict_path)
    else:
        # Prioridade 2: ZIP com "dicionario" no nome → extrair .xls/.xlsx de dentro
        nome_zip_dict = next(
            (a for a in arquivos_doc if _eh_zip_dicionario(a)),
            None,
        )
        if nome_zip_dict is None:
            raise FileNotFoundError(
                f"Dicionário PNADc não encontrado em {url_doc}.\n"
                f"Arquivos disponíveis: {arquivos_doc}"
            )

        zip_dict_local = output_dir / nome_zip_dict
        if not zip_dict_local.exists():
            logger.info("[Grupo 5] Baixando ZIP do dicionário: %s", nome_zip_dict)
            _download_arquivo(f"{url_doc}{nome_zip_dict}", zip_dict_local)
        else:
            logger.info("[Grupo 5] Cache hit (ZIP dicionário): %s", zip_dict_local.name)

        # Extrair o .xls/.xlsx de dentro do ZIP
        with zipfile.ZipFile(zip_dict_local) as zf:
            xls_members = [m for m in zf.namelist() if _eh_xls(m)]
            if not xls_members:
                raise FileNotFoundError(
                    f"Nenhum .xls/.xlsx encontrado dentro de {zip_dict_local.name}.\n"
                    f"Conteúdo: {zf.namelist()}"
                )
            # Prefere arquivo com "dicionario" no nome; fallback: primeiro .xls
            xls_member = next(
                (m for m in xls_members if "dicionario" in m.lower()), xls_members[0]
            )
            logger.info("[Grupo 5] Extraindo dicionário: %s", xls_member)
            zf.extract(xls_member, output_dir)
            # Arquivo pode estar em subpasta dentro do ZIP
            dict_path = output_dir / xls_member

    logger.info(
        "[Grupo 5] Arquivos prontos — microdados: %s | dicionário: %s",
        zip_path.name, dict_path.name,
    )
    return zip_path, dict_path


# ---------------------------------------------------------------------------
# Etapa 3b — Identificação do tipo de município e domínio V1029
# ---------------------------------------------------------------------------

# V1023 numeric codes in PNADC microdata (from dictionary)
_V1023_CAPITAL  = [1]
_V1023_RM_RIDE  = [2, 3]   # Resto da RM; Resto da RIDE
_V1023_INTERIOR = [4]       # Resto da UF


def _identificar_tipo_municipio(codigo_ibge: str) -> tuple[int, str, list[int]]:
    """
    Identifies the municipality type for PNADC domain selection.

    Uses geobr to determine whether the municipality is a state capital,
    part of a metropolitan/integrated development region, or an interior city.
    The returned V1023 codes filter the correct estimation domain (V1029)
    from PNADc microdata.

    Parameters
    ----------
    codigo_ibge : str
        7-digit IBGE municipality code.

    Returns
    -------
    (cod_uf, tipo, v1023_codigos) : tuple[int, str, list[int]]
        cod_uf        — 2-digit UF code (first 2 digits of codigo_ibge)
        tipo          — "capital" | "rm" | "interior"
        v1023_codigos — list of V1023 integer codes that identify this domain

    Notes
    -----
    - geobr.read_capitals(as_sf=False) avoids a known bug in the Python package
      where read_municipal_seat does not accept a show_progress keyword argument.
    - Metro area classification uses geobr.read_metro_area(year=2018), which
      covers both IBGE-recognized RMs and state-created RMs/RIDEs.
    - Fallback: if any geobr call fails, the function defaults to "interior".
    """
    import warnings

    codigo_ibge = str(codigo_ibge)
    cod_uf = int(codigo_ibge[:2])
    code_int = int(codigo_ibge)

    # ── Capital? ───────────────────────────────────────────────────────────
    is_capital = False
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            # as_sf=False avoids the buggy read_municipal_seat(show_progress=...) call
            caps = geobr.read_capitals(as_sf=False)
        is_capital = code_int in caps["code_muni"].values
        logger.info("[Grupo 5] Capital check %s → %s", codigo_ibge, is_capital)
    except Exception as exc:
        logger.warning("[Grupo 5] geobr.read_capitals falhou: %s — assumindo não-capital.", exc)

    if is_capital:
        logger.info("[Grupo 5] Tipo: capital → V1023 = %s", _V1023_CAPITAL)
        return cod_uf, "capital", _V1023_CAPITAL

    # ── Região Metropolitana / RIDE? ───────────────────────────────────────
    is_rm = False
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            rms = geobr.read_metro_area(year=2018, verbose=False)
        is_rm = code_int in rms["code_muni"].values
        if is_rm:
            nome_rm = rms.loc[rms["code_muni"] == code_int, "name_metro"].iloc[0]
            logger.info("[Grupo 5] Município em %s → tipo: rm", nome_rm)
    except Exception as exc:
        logger.warning("[Grupo 5] geobr.read_metro_area falhou: %s — assumindo interior.", exc)

    if is_rm:
        logger.info("[Grupo 5] Tipo: rm → V1023 = %s", _V1023_RM_RIDE)
        return cod_uf, "rm", _V1023_RM_RIDE

    logger.info("[Grupo 5] Tipo: interior → V1023 = %s", _V1023_INTERIOR)
    return cod_uf, "interior", _V1023_INTERIOR


# ---------------------------------------------------------------------------
# Etapa 3c — Leitura dos microdados e cálculo de estimativas
# ---------------------------------------------------------------------------

def _ler_dicionario(dict_path: Path) -> pd.DataFrame:
    """
    Parses the PNADC XLS data dictionary into a tidy DataFrame of column positions.

    Returns a DataFrame with columns: pos_ini (int), tamanho (int), variavel (str).
    Filters out section headers and category rows (which have no position/width).
    """
    engine = "xlrd" if str(dict_path).lower().endswith(".xls") else "openpyxl"
    df_raw = pd.read_excel(dict_path, header=None, engine=engine)
    df_raw.columns = [
        "pos_ini", "tamanho", "variavel",
        "q_n", "q_desc", "cat_tipo", "cat_desc", "periodo",
    ]
    mask = (
        pd.to_numeric(df_raw["pos_ini"], errors="coerce").notna()
        & pd.to_numeric(df_raw["tamanho"], errors="coerce").notna()
    )
    df_vars = df_raw[mask].copy()
    df_vars["pos_ini"] = pd.to_numeric(df_vars["pos_ini"]).astype(int)
    df_vars["tamanho"] = pd.to_numeric(df_vars["tamanho"]).astype(int)
    return df_vars[["pos_ini", "tamanho", "variavel"]].reset_index(drop=True)


def _calcular_estimativas(
    zip_path: Path,
    dict_path: Path,
    cod_uf: int,
    v1023_codigos: list[int],
    variaveis: list[str],
    fonte: str = "trimestral",
) -> tuple[pd.DataFrame, list[str]]:
    """
    Reads PNADc microdata, selects the municipality's estimation domain, and
    computes survey-weighted means via Taylor linearization (svy library).

    Parameters
    ----------
    zip_path : Path
        ZIP file with the fixed-width microdata text file.
    dict_path : Path
        XLS/XLSX data dictionary with column start positions and widths.
    cod_uf : int
        2-digit UF code used to pre-filter V1029 by prefix.
    v1023_codigos : list[int]
        V1023 integer codes identifying the municipality's PNADC domain
        (1=capital, 2-3=RM/RIDE, 4=interior).
    variaveis : list[str]
        Variables to estimate (must exist in the microdata).
    fonte : str
        Label for the 'fonte' column in the result (e.g. "trimestral").

    Returns
    -------
    (df_est, v1029_sel) : tuple[pd.DataFrame, list[str]]
        df_est     — estimates DataFrame: variavel, estimativa, erro_padrao,
                     ic_lower, ic_upper, n_obs, fonte
        v1029_sel  — list of V1029 domain codes selected for estimation

    Notes
    -----
    - All variables in PNADC fixed-width format are stored as text; this
      function converts them to numeric before applying survey weights.
    - Domain selection: filters V1029 by UF prefix, then selects domains whose
      predominant V1023 matches the expected type. Falls back to all UF domains
      if no match is found.
    - Variables absent from the microdata are skipped with a warning (not fatal).
    """
    import polars as pl
    import svy as _svy

    # ── 1. Parse dictionary → colspecs ────────────────────────────────────
    logger.info("[Grupo 5] Lendo dicionário: %s", dict_path.name)
    df_dict = _ler_dicionario(dict_path)

    # Always read design + geo vars plus requested vars
    # UF is used for filtering (avoids the V1029 leading-zero prefix issue)
    vars_leitura = list(dict.fromkeys(
        ["UF", "Estrato", "UPA", "V1028", "V1029", "V1023"] + variaveis
    ))
    df_sel = df_dict[df_dict["variavel"].isin(vars_leitura)].sort_values("pos_ini")
    ausentes_dict = [v for v in vars_leitura if v not in df_dict["variavel"].values]
    if ausentes_dict:
        logger.warning("[Grupo 5] Variáveis ausentes no dicionário: %s", ausentes_dict)

    colspecs = [
        (row.pos_ini - 1, row.pos_ini - 1 + row.tamanho)
        for _, row in df_sel.iterrows()
    ]
    names = list(df_sel["variavel"])

    # ── 2. Read fixed-width microdata from ZIP ─────────────────────────────
    with zipfile.ZipFile(zip_path) as zf:
        txt_members = [
            m for m in zf.namelist()
            if m.upper().endswith(".TXT") and "PNADC" in m.upper()
        ]
        if not txt_members:
            raise FileNotFoundError(
                f"Nenhum arquivo .txt de microdados encontrado em {zip_path.name}.\n"
                f"Conteúdo: {zf.namelist()}"
            )
        txt_member = txt_members[0]
        logger.info(
            "[Grupo 5] Lendo microdados: %s (pode demorar 1–3 min)...", txt_member
        )
        with zf.open(txt_member) as f:
            df = pd.read_fwf(
                f,
                colspecs=colspecs,
                names=names,
                dtype=str,
                encoding="latin-1",
            )

    logger.info("[Grupo 5] Microdados carregados: %d registros nacionais", len(df))

    # ── 3. Filter by UF column ─────────────────────────────────────────────
    # Use the UF column directly (str "27") — avoids the V1029 leading-zero
    # prefix ambiguity that was an issue in the R implementation (haven_labelled).
    uf_str = str(cod_uf).zfill(2)
    df_uf = df[df["UF"].str.strip() == uf_str].copy()
    logger.info("[Grupo 5] Registros na UF %s: %d", uf_str, len(df_uf))

    if df_uf.empty:
        ufs_presentes = sorted(df["UF"].str.strip().dropna().unique().tolist())
        raise ValueError(
            f"Nenhum registro para UF {uf_str} nos microdados.\n"
            f"UFs presentes (amostra): {ufs_presentes[:15]}"
        )

    # ── 4. Select estimation domain by V1023 ──────────────────────────────
    df_uf["_v1023"] = pd.to_numeric(df_uf["V1023"], errors="coerce")

    v1029_info: dict[str, dict] = {}
    for v1029_val, grp in df_uf.groupby("V1029"):
        v1023_counts = grp["_v1023"].dropna().astype(int).value_counts()
        matched = any(c in v1023_codigos for c in v1023_counts.index)
        v1029_info[str(v1029_val)] = {
            "n": len(grp),
            "v1023_vals": sorted(v1023_counts.index.tolist()),
            "match": matched,
        }
        logger.info(
            "[Grupo 5] V1029 %-12s | V1023: %-10s | n=%d | %s",
            v1029_val,
            str(sorted(v1023_counts.index.tolist())),
            len(grp),
            "[MATCH]" if matched else "",
        )

    v1029_sel = [v for v, info in v1029_info.items() if info["match"]]

    if not v1029_sel:
        logger.warning(
            "[Grupo 5] Nenhum V1029 com V1023 em %s — usando todos os V1029 da UF como fallback.",
            v1023_codigos,
        )
        v1029_sel = list(v1029_info.keys())

    df_dom = df_uf[df_uf["V1029"].isin(v1029_sel)].copy()
    logger.info(
        "[Grupo 5] Domínio selecionado: %d registros | V1029: %s",
        len(df_dom), v1029_sel,
    )

    # ── 5. Convert design variables to numeric ─────────────────────────────
    df_dom["V1028"] = pd.to_numeric(df_dom["V1028"], errors="coerce")
    df_dom["Estrato"] = df_dom["Estrato"].astype(str)
    df_dom["UPA"] = df_dom["UPA"].astype(str)

    # Convert target variables to numeric; keep only those with valid data
    vars_analisar = [v for v in variaveis if v in df_dom.columns]
    vars_ausentes = [v for v in variaveis if v not in df_dom.columns]
    for var in vars_ausentes:
        logger.warning("[Grupo 5] Variável ausente nos microdados: %s", var)

    for var in vars_analisar:
        df_dom[var] = pd.to_numeric(df_dom[var], errors="coerce")

    # ── 6. Survey-weighted estimation (svy / Taylor linearization) ─────────
    cols_svy = ["Estrato", "UPA", "V1028"] + vars_analisar
    df_svy = df_dom[cols_svy].dropna(subset=["V1028"])

    df_pl = pl.from_pandas(df_svy)
    design = _svy.Design(stratum="Estrato", psu="UPA", wgt="V1028")
    sample = _svy.Sample(df_pl, design)

    resultados = []
    for var in vars_analisar:
        n_validos = int(df_svy[var].notna().sum())
        if n_validos == 0:
            logger.warning("[Grupo 5] %s: sem observações válidas — pulando.", var)
            continue
        try:
            est = sample.estimation.mean(var)
            row = est.to_polars().to_pandas().iloc[0]
            resultados.append({
                "variavel":    var,
                "estimativa":  round(float(row["est"]), 6),
                "erro_padrao": round(float(row["se"]),  6),
                "ic_lower":    round(float(row["lci"]), 6),
                "ic_upper":    round(float(row["uci"]), 6),
                "n_obs":       n_validos,
                "fonte":       fonte,
            })
            logger.info(
                "[Grupo 5] %-10s = %.4f ± %.4f  (n=%d)  [%s]",
                var, row["est"], row["se"], n_validos, fonte,
            )
        except Exception as exc:
            logger.warning("[Grupo 5] svymean falhou para %s: %s", var, exc)

    df_est = pd.DataFrame(resultados)
    return df_est, v1029_sel

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
    Coleta estimativas PNADc para o domínio de estimação V1029 do município.

    Implementado em Python puro (sem dependência R). Substitui a versão anterior
    que chamava extrair_pnadc.R via subprocess (incompatível com SAC/Windows 11).

    Parâmetros
    ----------
    codigo_ibge : str
        Código IBGE de 7 dígitos. Ex: "2700300".
    limite_municipal : gpd.GeoDataFrame
        Não usado diretamente — mantido por compatibilidade com a assinatura padrão.
        O domínio geográfico é identificado via V1029/V1023 na PNADc.
    output_dir : Path
        Pasta de saída por município: data/raw/{uf}_{municipio}/pnadc/
        Os CSVs de estimativas e metadados são gravados aqui.
    db_conn : duckdb.DuckDBPyConnection
        Conexão aberta com o DuckDB do município.
    ano : int
        Ano da PNADc. Padrão: 2022.
    trimestre : int
        Trimestre (1-4). Padrão: 4.
    forcar : bool
        Se True, reprocessa mesmo que os CSVs já existam em output_dir.
    **kwargs
        cache_dir : Path | str — diretório para cachear o arquivo ZIP de microdados
            e o dicionário (compartilhado entre municípios). Padrão: output_dir/../../../cache_pnadc/

    Retorna
    -------
    dict
        {"status": "ok"|"erro", "camadas": [...], "mensagem": "..."}

    Notas
    -----
    - Na primeira execução baixa ~200 MB de microdados do FTP IBGE (5–15 min).
    - O ZIP de microdados é cacheado em cache_dir — reutilizado para outros municípios
      do mesmo trimestre sem novo download.
    - Variáveis S01xxx e VD5008 estão ausentes do produto trimestral PNADc 2022
      (confirmado em 2026-03-22); registradas nos metadados sem erro fatal.
    - VD5008 é uma variável derivada calculada pelo pacote PNADcIBGE; não está no
      arquivo de microdados de largura fixa.

    ⚠️ AVISO OBRIGATÓRIO: Estimativas válidas para a área de ponderação (domínio V1029).
       NÃO representam o município isoladamente.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Cache dir: shared across municipalities for the heavy microdata ZIP
    cache_dir = Path(kwargs.get("cache_dir") or output_dir.parents[2] / "cache_pnadc")
    cache_dir.mkdir(parents=True, exist_ok=True)

    csv_est  = output_dir / f"pnadc_{ano}T{trimestre}_estimativas.csv"
    csv_meta = output_dir / f"pnadc_{ano}T{trimestre}_metadados.csv"

    camadas_salvas: list[str] = []

    try:
        # ── 1. Cache check ─────────────────────────────────────────────────
        if csv_est.exists() and not forcar:
            logger.info(
                "[Grupo 5] CSV já existe (%s) — pulando processamento. "
                "Use forcar=True para reprocessar.",
                csv_est.name,
            )
        else:
            # ── 2. Download microdata + dictionary ─────────────────────────
            zip_path, dict_path = _baixar_microdados_pnadc(ano, trimestre, cache_dir)

            # ── 3. Municipality type → V1023 filter ───────────────────────
            cod_uf, tipo_mun, v1023_codigos = _identificar_tipo_municipio(codigo_ibge)

            # ── 4. Variables available in the trimestral microdata ─────────
            df_dict = _ler_dicionario(dict_path)
            vars_disponiveis = set(df_dict["variavel"].tolist())
            variaveis_alvo = [v for v in VARS_HAB if v in vars_disponiveis]
            vars_ausentes  = [v for v in VARS_HAB if v not in vars_disponiveis]

            if vars_ausentes:
                logger.warning(
                    "[Grupo 5] Variáveis ausentes no dicionário trimestral "
                    "(esperado — S01xxx e VD5008 não fazem parte dos microdados "
                    "de largura fixa do produto trimestral): %s",
                    vars_ausentes,
                )

            if not variaveis_alvo:
                raise ValueError(
                    "Nenhuma variável alvo disponível no dicionário PNADc. "
                    "Verifique o arquivo de dicionário."
                )

            # ── 5. Calculate survey estimates ──────────────────────────────
            df_est, v1029_sel = _calcular_estimativas(
                zip_path, dict_path, cod_uf, v1023_codigos,
                variaveis_alvo, fonte=f"{ano}T{trimestre}",
            )

            if df_est.empty:
                raise ValueError("Nenhuma estimativa calculada.")

            # ── 6. Add identifying columns ─────────────────────────────────
            df_est["ano"]         = ano
            df_est["trimestre"]   = trimestre
            df_est["codigo_ibge"] = codigo_ibge

            # ── 7. Save estimates CSV ──────────────────────────────────────
            df_est.to_csv(csv_est, index=False, encoding="utf-8")
            logger.info("[Grupo 5] Estimativas salvas: %s (%d variáveis)", csv_est.name, len(df_est))

            # ── 8. Build and save metadata ─────────────────────────────────
            aviso = (
                f"Estimativas válidas para o domínio V1029 ({'; '.join(v1029_sel)}). "
                f"NÃO representam o município {codigo_ibge} isoladamente."
            )
            logger.warning("[Grupo 5] %s", aviso)

            meta_rows = [
                ("fonte",               "IBGE PNADc trimestral — FTP microdados"),
                ("ano",                 str(ano)),
                ("trimestre",           str(trimestre)),
                ("codigo_ibge",         codigo_ibge),
                ("tipo_municipio",      tipo_mun),
                ("nivel_geografico",    "dominio_estimacao_v1029"),
                ("v1029_selecionados",  "; ".join(v1029_sel)),
                ("v1023_codigos_usados", str(v1023_codigos)),
                ("variaveis_estimadas", "; ".join(df_est["variavel"].tolist())),
                ("variaveis_ausentes",  "; ".join(vars_ausentes)),
                ("n_obs_dominio",       str(df_est["n_obs"].sum() if not df_est.empty else 0)),
                ("aviso",               aviso),
            ]
            df_meta = pd.DataFrame(meta_rows, columns=["chave", "valor"])
            df_meta.to_csv(csv_meta, index=False, encoding="utf-8")

        # ── 9. Read CSVs → DuckDB ──────────────────────────────────────────
        df_est_final = pd.read_csv(csv_est, encoding="utf-8")
        if df_est_final.empty:
            raise ValueError("CSV de estimativas está vazio.")

        salvar_dataframe(db_conn, df_est_final, "pnadc_estimativas")
        camadas_salvas.append("pnadc_estimativas")
        logger.info(
            "[Grupo 5] pnadc_estimativas: %d linhas, colunas: %s",
            len(df_est_final), list(df_est_final.columns),
        )

        if csv_meta.exists():
            df_meta_final = pd.read_csv(csv_meta, encoding="utf-8")
            salvar_dataframe(db_conn, df_meta_final, "pnadc_metadados")
            camadas_salvas.append("pnadc_metadados")

            aviso_db = df_meta_final.loc[df_meta_final["chave"] == "aviso", "valor"]
            if not aviso_db.empty:
                logger.warning("[Grupo 5] %s", aviso_db.iloc[0])

        return {
            "status": "ok",
            "camadas": camadas_salvas,
            "mensagem": (
                f"PNADc {ano}T{trimestre} processada: "
                f"{len(df_est_final)} variáveis estimadas."
            ),
        }

    except Exception as exc:
        logger.error("[Grupo 5] Erro: %s", exc, exc_info=True)
        return {
            "status": "erro",
            "camadas": camadas_salvas,
            "mensagem": str(exc),
        }
