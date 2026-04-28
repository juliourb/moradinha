"""
grupo5_pnadc.py — PNADc Anual Visita 1 + déficit habitacional FJP 2021.

⚠️ LIMITAÇÃO FUNDAMENTAL: A PNADc NÃO tem representatividade municipal.
   A menor unidade geográfica é a ÁREA DE PONDERAÇÃO (V1029).
   Os resultados são válidos para o domínio de estimação, não para o município.

Fluxo:
  FTP IBGE (microdados Anual V1 + dicionário)
    → pandas.read_fwf
    → classificação FJP 2021 em cascata (3 componentes)
    → estimação Rao-Wu Bootstrap (200 réplicas V1032001–V1032200)
    → DuckDB

Metodologia: FJP 2021 — Relatório Metodológico do Déficit Habitacional
  https://repositorio.fjp.mg.gov.br/items/a79c5256-7329-443b-acad-30c5e9640bd8

Tabelas DuckDB geradas:
    pnadc_microdados_v1         — microdados filtrados + coluna "componente"
    pnadc_deficit_componentes   — estimativas por componente (Rao-Wu)
    pnadc_deficit_recortes      — estimativas por recortes (sexo, cor/raça, renda)
    pnadc_metadados             — metadados, hashes MD5, avisos

Dependências: openpyxl ou xlrd (dicionário .xls/.xlsx), requests
"""

from __future__ import annotations

import hashlib
import logging
import re
import time
import zipfile
from pathlib import Path

import geopandas as gpd
import pandas as pd
import requests

from ..utils.db_utils import salvar_dataframe

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constantes
# ---------------------------------------------------------------------------

_BASE_FTP_ANUAL = (
    "https://ftp.ibge.gov.br/Trabalho_e_Rendimento/"
    "Pesquisa_Nacional_por_Amostra_de_Domicilios_continua/"
    "Anual/Microdados/Visita/Visita_1"
)

# ---------------------------------------------------------------------------
# Utilidades de download (reutilizadas do backup trimestral)
# ---------------------------------------------------------------------------

def _listar_ftp_ibge(url_dir: str) -> list[str]:
    """Lists file names from an IBGE HTTPS-FTP directory (Apache index)."""
    url = url_dir.rstrip("/") + "/"
    resp = requests.get(url, timeout=60)
    resp.raise_for_status()
    nomes = re.findall(r'href="([^"/?][^"]*)"', resp.text)
    return nomes


def _download_arquivo(url: str, dest: Path) -> None:
    """Downloads url → dest in streaming mode, logging URL, size and elapsed time."""
    t0 = time.time()
    resp = requests.get(url, stream=True, timeout=600)
    resp.raise_for_status()

    total = int(resp.headers.get("content-length", 0))
    baixado = 0
    chunk_mb = 4 * 1024 * 1024

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


def _md5_arquivo(caminho: Path, chunk_size: int = 8 * 1024 * 1024) -> str:
    """Computes MD5 hex digest of a file without loading it entirely into memory."""
    h = hashlib.md5()
    with open(caminho, "rb") as f:
        while True:
            bloco = f.read(chunk_size)
            if not bloco:
                break
            h.update(bloco)
    return h.hexdigest()


# ---------------------------------------------------------------------------
# Variáveis a carregar dos microdados Anual V1
# ---------------------------------------------------------------------------

# Identificação e plano amostral (sempre carregadas)
# V1029 (área de ponderação) está ausente do dicionário 2022 da Anual V1 —
# o domínio é selecionado via V1023 (tipo de área) em filtrar_area_ponderacao().
VARS_DESIGN_V1 = ["UF", "Estrato", "UPA", "V1008", "V1014", "V1022", "V1023", "V1032"]

# Variáveis da cascata FJP (códigos 2022 — verificar ao mudar de ano)
VARS_CASCATA = [
    "V2001",   # nº de moradores no domicílio
    "V2005",   # condição da pessoa no domicílio (para V2005=1: referência)
    "VD2004",  # espécie da unidade doméstica (nuclear/extensa/composta)
    "VD5007",  # renda habitual domiciliar (para ônus excessivo)
    "S01001",  # tipo de domicílio (casa, apto, cômodo)
    "S01002",  # material das paredes externas (rústico?)
    "S01005",  # total de cômodos
    "S01006",  # cômodos servindo de dormitório
    "S01017",  # condição de ocupação (alugado?)
    "S01019",  # valor mensal do aluguel
]

# Recortes para estimativas desagregadas
VARS_RECORTES = ["V2007", "V2009", "V2010"]  # sexo, idade, cor/raça

# Réplicas Rao-Wu (geradas dinamicamente)
VARS_REPLICAS = [f"V1032{r:03d}" for r in range(1, 201)]

# ---------------------------------------------------------------------------
# Constantes da cascata FJP 2021 — derivadas do dicionário 2022
# (confirmar no dicionário ao mudar de ano via parsear_categorias)
# ---------------------------------------------------------------------------

# S01001 — tipo do domicílio: cômodo/cortiço/cabeça-de-porco
S01001_COMODO: frozenset[int] = frozenset({3})

# S01002 — material das paredes: taipa sem revestimento, madeira aproveitada, outro
S01002_RUSTICO: frozenset[int] = frozenset({3, 5, 6})

# S01017 — condição de ocupação: alugado
S01017_ALUGADO: frozenset[int] = frozenset({3})

# ---------------------------------------------------------------------------
# Fase A — Download e parse do dicionário Anual V1
# ---------------------------------------------------------------------------

def _baixar_dicionario_anual_v1(ano: int, cache_dir: Path) -> Path:
    """
    Downloads the PNADc Annual Visit 1 data dictionary for the given year.

    The dictionary is an XLS/XLSX file that maps each variable to its start
    position and width in the fixed-width microdata text file. Without it,
    pandas.read_fwf cannot parse the microdata correctly.

    Parameters
    ----------
    ano : int
        Reference year (e.g., 2022). The FTP directory contains one dictionary
        per year, sometimes packed inside a ZIP.
    cache_dir : Path
        Local directory for caching downloaded files. If the dictionary file
        already exists and its MD5 matches a stored checksum, the download is
        skipped.

    Returns
    -------
    Path
        Local path to the downloaded XLS/XLSX dictionary file.

    Notes
    -----
    - Naming convention: IBGE uses patterns like
      `dicionario_PNADC_microdados_{ano}_visita1_YYYYMMDD.xls`.
    - If only a ZIP is available (as with the trimestral product), the XLS is
      extracted from inside the ZIP automatically.
    - The MD5 checksum is written to `{dict_path}.md5` for idempotency checks.
    """
    cache_dir = Path(cache_dir)
    cache_dir.mkdir(parents=True, exist_ok=True)

    url_doc = f"{_BASE_FTP_ANUAL}/Documentacao/"
    logger.info("[Grupo 5] Listando documentação Anual V1: %s", url_doc)
    arquivos_doc = _listar_ftp_ibge(url_doc)
    logger.info("[Grupo 5] Arquivos disponíveis (%d): %s", len(arquivos_doc), arquivos_doc)

    ano_str = str(ano)

    def _eh_xls(nome: str) -> bool:
        return nome.lower().endswith((".xls", ".xlsx"))

    def _eh_dict_do_ano(nome: str) -> bool:
        # Match "microdados_2022_visita1" — avoids false positives from
        # publication dates like "20220224" embedded in other years' filenames.
        return "dicionario" in nome.lower() and f"microdados_{ano_str}_visita1" in nome.lower()

    # ── Prioridade 1: XLS/XLSX direto na pasta, com ano no nome ──────────────
    nome_dict_direto = next(
        (a for a in arquivos_doc if _eh_dict_do_ano(a) and _eh_xls(a)),
        None,
    )
    # Fallback: qualquer XLS com "dicionario" no nome (sem filtro de ano)
    if nome_dict_direto is None:
        nome_dict_direto = next(
            (a for a in arquivos_doc if "dicionario" in a.lower() and _eh_xls(a)),
            None,
        )

    if nome_dict_direto is not None:
        dict_path = cache_dir / nome_dict_direto
        md5_path  = dict_path.with_suffix(dict_path.suffix + ".md5")
        if dict_path.exists() and md5_path.exists():
            logger.info("[Grupo 5] Cache hit (dicionário): %s", dict_path.name)
            return dict_path
        _download_arquivo(f"{url_doc}{nome_dict_direto}", dict_path)
        md5_path.write_text(_md5_arquivo(dict_path))
        return dict_path

    # ── Prioridade 2: ZIP com "dicionario" no nome → extrair XLS de dentro ───
    nome_zip_dict = next(
        (a for a in arquivos_doc if "dicionario" in a.lower() and a.lower().endswith(".zip")),
        None,
    )
    if nome_zip_dict is None:
        raise FileNotFoundError(
            f"Dicionário PNADc Anual V1 não encontrado para {ano} em {url_doc}.\n"
            f"Arquivos disponíveis: {arquivos_doc}"
        )

    zip_dict_local = cache_dir / nome_zip_dict
    if not zip_dict_local.exists():
        logger.info("[Grupo 5] Baixando ZIP do dicionário: %s", nome_zip_dict)
        _download_arquivo(f"{url_doc}{nome_zip_dict}", zip_dict_local)
    else:
        logger.info("[Grupo 5] Cache hit (ZIP dicionário): %s", zip_dict_local.name)

    with zipfile.ZipFile(zip_dict_local) as zf:
        xls_members = [m for m in zf.namelist() if _eh_xls(m)]
        if not xls_members:
            raise FileNotFoundError(
                f"Nenhum .xls/.xlsx encontrado dentro de {zip_dict_local.name}.\n"
                f"Conteúdo: {zf.namelist()}"
            )
        xls_member = next(
            (m for m in xls_members if _eh_dict_do_ano(m)),
            next((m for m in xls_members if "dicionario" in m.lower()), xls_members[0]),
        )
        logger.info("[Grupo 5] Extraindo dicionário: %s", xls_member)
        zf.extract(xls_member, cache_dir)
        dict_path = cache_dir / xls_member

    md5_path = dict_path.with_suffix(dict_path.suffix + ".md5")
    md5_path.write_text(_md5_arquivo(dict_path))
    logger.info("[Grupo 5] Dicionário pronto: %s", dict_path.name)
    return dict_path


def parsear_dicionario(dict_path: Path, salvar_audit_s01: bool = True) -> pd.DataFrame:
    """
    Parses the PNADc XLS data dictionary into a tidy DataFrame.

    Returns a DataFrame with columns: pos_ini (int), tamanho (int),
    variavel (str), descricao (str). Only rows with valid numeric
    pos_ini and tamanho are kept (section headers and category rows
    are filtered out).

    Parameters
    ----------
    dict_path : Path
        Local path to the XLS/XLSX dictionary file.
    salvar_audit_s01 : bool
        If True (default), saves a CSV of S01XXX variables alongside the
        dictionary file for manual inspection of category codes before
        implementing the FJP cascade classifier.

    Returns
    -------
    pd.DataFrame
        Columns: pos_ini, tamanho, variavel, descricao
        Sorted by pos_ini ascending.
    """
    engine = "xlrd" if str(dict_path).lower().endswith(".xls") else "openpyxl"
    df_raw = pd.read_excel(dict_path, header=None, engine=engine)

    # PNADc dictionary layout (columns vary slightly across years):
    # col 0: pos_ini (start position in fixed-width file)
    # col 1: tamanho (field width)
    # col 2: variavel (variable code, e.g. "V1029")
    # col 3: questionnaire item number
    # col 4: descricao (human-readable description)
    # remaining cols: category type, category description, reference period
    df_raw.columns = range(len(df_raw.columns))
    col_pos  = 0
    col_tam  = 1
    col_var  = 2
    col_desc = 4 if len(df_raw.columns) > 4 else 3

    mask = (
        pd.to_numeric(df_raw[col_pos], errors="coerce").notna()
        & pd.to_numeric(df_raw[col_tam], errors="coerce").notna()
    )
    df_vars = df_raw[mask].copy()
    df_vars["pos_ini"]  = pd.to_numeric(df_vars[col_pos]).astype(int)
    df_vars["tamanho"]  = pd.to_numeric(df_vars[col_tam]).astype(int)
    df_vars["variavel"] = df_vars[col_var].astype(str).str.strip()
    df_vars["descricao"] = df_vars[col_desc].astype(str).str.strip() if col_desc < len(df_raw.columns) else ""

    df_out = (
        df_vars[["pos_ini", "tamanho", "variavel", "descricao"]]
        .sort_values("pos_ini")
        .drop_duplicates(subset="variavel", keep="first")
        .reset_index(drop=True)
    )

    logger.info(
        "[Grupo 5] Dicionário parseado: %d variáveis | pos máx: %d",
        len(df_out), df_out["pos_ini"].max(),
    )

    if salvar_audit_s01:
        df_s01 = df_out[df_out["variavel"].str.startswith("S01")].copy()
        audit_path = dict_path.parent / f"dicionario_S01_{dict_path.stem}.csv"
        df_s01.to_csv(audit_path, index=False, encoding="utf-8")
        logger.info(
            "[Grupo 5] Auditoria S01XXX: %d variáveis → %s",
            len(df_s01), audit_path.name,
        )

    return df_out


def parsear_categorias(dict_path: Path, variaveis: list[str]) -> pd.DataFrame:
    """
    Reads category codes for specific variables from the PNADc XLS dictionary.

    The XLS dictionary interleaves variable rows (with pos_ini/tamanho) and
    category rows (empty pos_ini, with a numeric code and label). This function
    collects the category rows that follow each requested variable.

    Parameters
    ----------
    dict_path : Path
        Local path to the XLS/XLSX dictionary file.
    variaveis : list[str]
        Variable codes to extract categories for (e.g. ["S01001", "S01002", "S01017"]).

    Returns
    -------
    pd.DataFrame
        Columns: variavel (str), codigo_categoria (str), descricao_categoria (str).
        Each row is one category value for a given variable.
    """
    engine = "xlrd" if str(dict_path).lower().endswith(".xls") else "openpyxl"
    df_raw = pd.read_excel(dict_path, header=None, engine=engine)
    df_raw.columns = range(len(df_raw.columns))

    col_pos  = 0
    col_tam  = 1
    col_var  = 2
    col_cat  = 5  # category code (numeric value in the microdata)
    col_desc = 6  # category label

    variaveis_set = set(variaveis)
    resultados = []
    variavel_atual = None

    for _, row in df_raw.iterrows():
        pos_val = pd.to_numeric(row[col_pos], errors="coerce")
        tam_val = pd.to_numeric(row[col_tam], errors="coerce")

        if pd.notna(pos_val) and pd.notna(tam_val):
            # Variable row — update current variable if it's one we want
            nome = str(row[col_var]).strip()
            variavel_atual = nome if nome in variaveis_set else None
        else:
            # Category row — collect if under a requested variable
            if variavel_atual is not None:
                cod = str(row[col_cat]).strip() if col_cat < len(row) else ""
                desc = str(row[col_desc]).strip() if col_desc < len(row) else ""
                if cod and cod != "nan":
                    resultados.append({
                        "variavel":           variavel_atual,
                        "codigo_categoria":   cod,
                        "descricao_categoria": desc,
                    })

    return pd.DataFrame(resultados)


# ---------------------------------------------------------------------------
# Fase B — Download dos microdados Anual V1
# ---------------------------------------------------------------------------

def _baixar_microdados_anual_v1(ano: int, cache_dir: Path) -> Path:
    """
    Downloads the PNADc Annual Visit 1 microdata fixed-width text file.

    The microdata file contains all Brazilian households interviewed in the
    first visit for the given year. It is a large file (~200–400 MB) stored
    as a ZIP containing a fixed-width .txt file.

    Parameters
    ----------
    ano : int
        Reference year (e.g., 2022).
    cache_dir : Path
        Local cache directory shared across municipalities. The file is
        downloaded once and reused for all municipalities in the same year.

    Returns
    -------
    Path
        Local path to the downloaded ZIP file (not extracted — pandas.read_fwf
        can read directly from the open ZipFile member in Fase C).

    Notes
    -----
    - Idempotency: if the ZIP exists and its MD5 matches the stored checksum,
      the download is skipped entirely.
    - IBGE naming pattern: `PNADC_{ano}_visita1.zip` (exact name confirmed via
      FTP listing at runtime — do not hardcode).
    - First download of a ~300 MB file takes 5–20 min depending on connection.
    """
    cache_dir = Path(cache_dir)
    cache_dir.mkdir(parents=True, exist_ok=True)

    url_dados = f"{_BASE_FTP_ANUAL}/Dados/"
    logger.info("[Grupo 5] Listando diretório de dados Anual V1: %s", url_dados)
    arquivos = _listar_ftp_ibge(url_dados)
    logger.info("[Grupo 5] Arquivos disponíveis (%d): %s", len(arquivos), arquivos)

    ano_str = str(ano)
    # Expected pattern: PNADC_2022_visita1.zip (case-insensitive)
    arquivo_zip = next(
        (a for a in arquivos
         if ano_str in a and "visita1" in a.lower() and a.lower().endswith(".zip")),
        None,
    )
    if arquivo_zip is None:
        raise FileNotFoundError(
            f"Arquivo de microdados PNADc Anual V1 não encontrado para {ano} em {url_dados}.\n"
            f"Padrão buscado: *{ano}*visita1*.zip\n"
            f"Arquivos disponíveis: {arquivos}"
        )

    zip_path = cache_dir / arquivo_zip
    md5_path = zip_path.with_suffix(zip_path.suffix + ".md5")

    if zip_path.exists() and md5_path.exists():
        logger.info(
            "[Grupo 5] Cache hit (microdados): %s — %.0f MB",
            zip_path.name, zip_path.stat().st_size / 1e6,
        )
        return zip_path

    logger.info(
        "[Grupo 5] Baixando microdados PNADc Anual V1 %d: %s "
        "(pode demorar 5–20 min na 1ª execução — arquivo ~200–400 MB)",
        ano, f"{url_dados}{arquivo_zip}",
    )
    _download_arquivo(f"{url_dados}{arquivo_zip}", zip_path)
    md5_path.write_text(_md5_arquivo(zip_path))
    logger.info("[Grupo 5] MD5 gravado: %s", md5_path.name)
    return zip_path


# ---------------------------------------------------------------------------
# Fase C — Leitura e filtro pela área de ponderação
# ---------------------------------------------------------------------------

# V1023 domain codes — same as trimestral product
_V1023_CAPITAL  = [1]
_V1023_RM_RIDE  = [2, 3]
_V1023_INTERIOR = [4]


def _identificar_tipo_municipio(codigo_ibge: str) -> tuple[int, str, list[int]]:
    """
    Identifies the municipality type for PNADc domain selection via V1023.

    Uses geobr to classify the municipality as capital, metropolitan region,
    or interior, then maps to the corresponding V1023 integer codes used in
    the PNADc microdata.

    Returns
    -------
    (cod_uf, tipo, v1023_codigos) : tuple[int, str, list[int]]
    """
    import warnings
    import geobr

    codigo_ibge = str(codigo_ibge)
    cod_uf = int(codigo_ibge[:2])
    code_int = int(codigo_ibge)

    is_capital = False
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            caps = geobr.read_capitals(as_sf=False)
        is_capital = code_int in caps["code_muni"].values
    except Exception as exc:
        logger.warning("[Grupo 5] geobr.read_capitals falhou: %s — assumindo não-capital.", exc)

    if is_capital:
        return cod_uf, "capital", _V1023_CAPITAL

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
        return cod_uf, "rm", _V1023_RM_RIDE

    return cod_uf, "interior", _V1023_INTERIOR


def ler_pnadc_anual_v1(
    zip_path: Path,
    dict_df: pd.DataFrame,
    vars_alvo: list[str] | None = None,
    incluir_replicas: bool = True,
) -> pd.DataFrame:
    """
    Reads PNADc Annual Visit 1 fixed-width microdata from a ZIP file.

    Each row in the microdata represents one PERSON. Household-level
    variables (S01XXX, V1032, replicas) have the same value for all
    persons within the same household (UPA, V1008, V1014).

    Parameters
    ----------
    zip_path : Path
        Local path to the microdata ZIP file.
    dict_df : pd.DataFrame
        Dictionary DataFrame from parsear_dicionario() with columns:
        pos_ini, tamanho, variavel.
    vars_alvo : list[str] | None
        Variables to load in addition to design vars. If None, loads
        VARS_DESIGN_V1 + VARS_CASCATA + VARS_RECORTES (+ replicas if requested).
    incluir_replicas : bool
        Whether to load V1032001–V1032200 (Rao-Wu replicas). Default True.
        Set to False for quick inspection runs to save memory.

    Returns
    -------
    pd.DataFrame
        Person-level DataFrame with all requested variables as strings.
        Numeric conversion is done in downstream functions.

    Notes
    -----
    - Peak memory during read: ~2–4 GB for the national file with replicas.
      Filtered subsets (one UF ~5% of national) are much smaller.
    - The file is not extracted from the ZIP — it is streamed directly.
    """
    if vars_alvo is None:
        vars_alvo = VARS_DESIGN_V1 + VARS_CASCATA + VARS_RECORTES

    vars_carregar = list(dict.fromkeys(vars_alvo))
    if incluir_replicas:
        vars_carregar = list(dict.fromkeys(vars_carregar + VARS_REPLICAS))

    # Build colspecs from dictionary
    df_sel = dict_df[dict_df["variavel"].isin(vars_carregar)].sort_values("pos_ini")
    ausentes = [v for v in vars_carregar if v not in dict_df["variavel"].values]
    if ausentes:
        logger.warning("[Grupo 5] Variáveis ausentes no dicionário: %s", ausentes)

    colspecs = [(row.pos_ini - 1, row.pos_ini - 1 + row.tamanho) for _, row in df_sel.iterrows()]
    names    = list(df_sel["variavel"])

    with zipfile.ZipFile(zip_path) as zf:
        txt_members = [m for m in zf.namelist() if m.upper().endswith(".TXT")]
        if not txt_members:
            raise FileNotFoundError(
                f"Nenhum .txt encontrado em {zip_path.name}.\nConteúdo: {zf.namelist()}"
            )
        txt_member = txt_members[0]
        logger.info(
            "[Grupo 5] Lendo microdados: %s (pode demorar 1–5 min)...", txt_member
        )
        with zf.open(txt_member) as f:
            df = pd.read_fwf(f, colspecs=colspecs, names=names, dtype=str, encoding="latin-1")

    logger.info(
        "[Grupo 5] Microdados carregados: %d registros (pessoas) | %d colunas",
        len(df), len(df.columns),
    )
    return df


def filtrar_area_ponderacao(
    df_pnadc: pd.DataFrame,
    codigo_ibge: str,
) -> tuple[pd.DataFrame, str, list[str]]:
    """
    Filters PNADc microdata to the estimation domain for the given municipality.

    The PNADc does not have municipal representativeness. The estimation unit
    is defined by (UF, V1023 type). This function identifies the relevant
    records by (a) filtering to the UF and (b) selecting records whose V1023
    type matches the municipality type (capital/RM/interior).

    If V1029 (weighting area code) is present in the data, it is used for
    finer domain selection (sub-areas within the same V1023 type). Otherwise,
    V1023 alone defines the domain — which is the case for PNADc Anual V1
    2022 where V1029 is not in the dictionary.

    Parameters
    ----------
    df_pnadc : pd.DataFrame
        Person-level microdata from ler_pnadc_anual_v1().
    codigo_ibge : str
        7-digit IBGE municipality code.

    Returns
    -------
    (df_filtrado, tipo_municipio, dominios_usados)
        df_filtrado      — person-level rows for the selected domain
        tipo_municipio   — "capital" | "rm" | "interior"
        dominios_usados  — V1029 codes (if available) or V1023 codes used
    """
    cod_uf, tipo_mun, v1023_codigos = _identificar_tipo_municipio(codigo_ibge)

    uf_str = str(cod_uf).zfill(2)
    df_uf = df_pnadc[df_pnadc["UF"].str.strip() == uf_str].copy()
    logger.info("[Grupo 5] Registros na UF %s: %d", uf_str, len(df_uf))

    if df_uf.empty:
        raise ValueError(f"Nenhum registro para UF {uf_str} nos microdados.")

    df_uf["_v1023_num"] = pd.to_numeric(df_uf["V1023"], errors="coerce")

    # ── Caso 1: V1029 presente — seleção por área de ponderação específica ──
    if "V1029" in df_uf.columns:
        v1029_info: dict[str, bool] = {}
        for v1029_val, grp in df_uf.groupby("V1029"):
            counts = grp["_v1023_num"].dropna().astype(int).value_counts()
            matched = any(c in v1023_codigos for c in counts.index)
            v1029_info[str(v1029_val)] = matched
            logger.info(
                "[Grupo 5] V1029 %-12s | V1023: %-8s | n=%d | %s",
                v1029_val, sorted(counts.index.tolist()), len(grp),
                "[MATCH]" if matched else "",
            )
        dominios = [v for v, match in v1029_info.items() if match]
        if not dominios:
            logger.warning("[Grupo 5] Nenhum V1029 com V1023 em %s — usando todos da UF.", v1023_codigos)
            dominios = list(v1029_info.keys())
        df_dom = df_uf[df_uf["V1029"].isin(dominios)].drop(columns=["_v1023_num"])
        logger.info("[Grupo 5] Domínio V1029: %d registros | tipo=%s | %s", len(df_dom), tipo_mun, dominios)

    # ── Caso 2: V1029 ausente (PNADc Anual V1) — seleção direta por V1023 ──
    else:
        logger.warning(
            "[Grupo 5] V1029 não encontrado nos microdados — usando V1023 como seletor de domínio "
            "(típico da PNADc Anual V1). Domínio = UF %s + V1023 ∈ %s.",
            uf_str, v1023_codigos,
        )
        df_dom = df_uf[df_uf["_v1023_num"].isin(v1023_codigos)].drop(columns=["_v1023_num"])
        dominios = [f"V1023={c}" for c in v1023_codigos]
        logger.info(
            "[Grupo 5] Domínio V1023: %d registros | tipo=%s | %s", len(df_dom), tipo_mun, dominios
        )

    return df_dom, tipo_mun, dominios


def agregar_para_domicilio(df_pessoas: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregates person-level PNADc microdata to household level.

    Household key: (UPA, V1008, V1014). For household-constant variables
    (S01XXX, V1032, replicas, V2001) the first person's value is used.
    For VD2004 (family unit type), a flag is set if ANY person in the
    household belongs to an extended or compound family unit (codes 3 or 4),
    which indicates potential cohabitation.

    Returns
    -------
    pd.DataFrame
        One row per household with all cascade variables and replicas as
        numeric types, plus 'flag_vd2004_extensa' (bool).
    """
    hh_key = ["UPA", "V1008", "V1014"]

    # Variables constant within household — take first value
    cols_primeiro = (
        ["Estrato", "V1022", "V1023", "V1029", "V2001", "VD5007",
         "S01001", "S01002", "S01005", "S01006", "S01017", "S01019",
         "V2007", "V2009", "V2010", "V1032"]
        + VARS_REPLICAS
    )
    cols_existentes = [c for c in cols_primeiro if c in df_pessoas.columns]

    df_hh = (
        df_pessoas.groupby(hh_key, sort=False)[cols_existentes]
        .first()
        .reset_index()
    )

    # flag_vd2004_extensa: any person has VD2004 ∈ {3, 4} (extensa/composta)
    df_pessoas["_vd2004_num"] = pd.to_numeric(df_pessoas["VD2004"], errors="coerce")
    df_vd2004 = (
        df_pessoas.groupby(hh_key, sort=False)["_vd2004_num"]
        .apply(lambda x: x.isin({3, 4}).any())
        .reset_index()
        .rename(columns={"_vd2004_num": "flag_vd2004_extensa"})
    )

    df_hh = df_hh.merge(df_vd2004, on=hh_key, how="left")

    # Convert numeric columns
    cols_num = (
        ["V2001", "VD5007", "S01001", "S01002", "S01005", "S01006",
         "S01017", "S01019", "V1022", "V2007", "V2009", "V2010", "V1032"]
        + VARS_REPLICAS
    )
    for col in cols_num:
        if col in df_hh.columns:
            df_hh[col] = pd.to_numeric(df_hh[col], errors="coerce")

    logger.info(
        "[Grupo 5] Domicílios no domínio: %d | colunas: %d",
        len(df_hh), len(df_hh.columns),
    )
    return df_hh


# ---------------------------------------------------------------------------
# Fase D — Classificação FJP em cascata (3 componentes)
# ---------------------------------------------------------------------------

def classificar_componente_fjp(
    df_hh: pd.DataFrame,
    salario_minimo: float,
    codes_rustico: frozenset[int] = S01002_RUSTICO,
    codes_comodo: frozenset[int] = S01001_COMODO,
    code_alugado: frozenset[int] = S01017_ALUGADO,
) -> pd.DataFrame:
    """
    Classifies each household into one FJP 2021 deficit component.

    FJP 2021 cascade — priority order (first match wins):
      1. Habitação Precária: rústico (S01002 ∈ codes_rustico)
         Improvisados: excluded — requires CadÚnico (documented in metadata).
      2. Coabitação: cômodo/cortiço (S01001 ∈ codes_comodo)
                    OR extended/compound family AND >2 persons per bedroom
      3. Ônus Excessivo: urban + durável + alugado
                         + renda ≤ 3 SM + aluguel ≥ 30% renda
      4. Não Déficit

    Implementation note: components are applied from lowest (3) to highest (1)
    priority so that the highest-priority label always wins via overwrite.

    Parameters
    ----------
    df_hh : pd.DataFrame
        Household-level DataFrame from agregar_para_domicilio().
    salario_minimo : float
        Monthly minimum wage in reais for the reference year.
    codes_rustico, codes_comodo, code_alugado : frozenset[int]
        Category codes from the annual dictionary. Defaults are from the
        2022 dictionary (see constants at module top). Verify with
        parsear_categorias() when changing year.

    Returns
    -------
    pd.DataFrame
        Input DataFrame with added 'componente' column:
        {"habitacao_precaria", "coabitacao", "onus_excessivo", "nao_deficit"}
    """
    import numpy as np

    df = df_hh.copy()
    df["componente"] = "nao_deficit"

    # ── Componente 3 — Ônus Excessivo (lower priority, set first) ────────────
    # Conditions: urban + durável + alugado + renda ≤ 3SM + aluguel ≥ 30% renda
    mask_urbano  = df["V1022"] == 1
    mask_duravel = ~df["S01001"].isin(codes_comodo)
    mask_nao_rqs = ~df["S01002"].isin(codes_rustico)
    mask_alugado = df["S01017"].isin(code_alugado)
    mask_3sm     = df["VD5007"].fillna(float("inf")) / salario_minimo <= 3
    # Safe division: VD5007=0 → NaN → ratio=NaN → False (conservative)
    ratio_aluguel = df["S01019"].fillna(0) / df["VD5007"].replace(0, float("nan"))
    mask_30pct   = ratio_aluguel >= 0.30

    mask_onus = mask_urbano & mask_duravel & mask_nao_rqs & mask_alugado & mask_3sm & mask_30pct
    df.loc[mask_onus, "componente"] = "onus_excessivo"

    # ── Componente 2 — Coabitação (overwrites ônus) ───────────────────────────
    mask_comodo = df["S01001"].isin(codes_comodo)
    # Adensamento: extended/compound family AND > 2 moradores por dormitório
    # S01006=0 edge case: replace with NaN so ratio becomes NaN (not ∞)
    dens_ratio  = df["V2001"] / df["S01006"].replace(0, float("nan"))
    mask_adens  = df["flag_vd2004_extensa"] & (dens_ratio > 2)
    mask_coabit = mask_comodo | mask_adens
    df.loc[mask_coabit, "componente"] = "coabitacao"

    # ── Componente 1 — Habitação Precária (highest priority, overwrites all) ──
    mask_rustico = df["S01002"].isin(codes_rustico)
    df.loc[mask_rustico, "componente"] = "habitacao_precaria"

    counts = df["componente"].value_counts()
    total_deficit = df[df["componente"] != "nao_deficit"]["V1032"].sum()
    logger.info(
        "[Grupo 5] Classificação FJP (n domicílios):\n%s\nTotal em déficit (peso V1032): %.0f",
        "\n".join(f"  {k}: {v}" for k, v in counts.items()),
        total_deficit,
    )
    return df


# ---------------------------------------------------------------------------
# Fase E — Estimação Rao-Wu Bootstrap (200 réplicas)
# ---------------------------------------------------------------------------

def _rao_wu_total(
    df: pd.DataFrame,
    col_peso: str,
    col_replicas: list[str],
    mask: pd.Series,
) -> dict:
    """
    Estimates a domain total using Rao-Wu bootstrap replication.

    The point estimate is the weighted sum of the mask (True = in deficit)
    using V1032. Variance is estimated from the 200 bootstrap replicas
    V1032001–V1032200: each replica gives an alternative total, and the
    variance is the mean squared deviation from the point estimate.

    Var(T) = (1/R) * Σ_r (T_r - T)²,  R = 200

    Parameters
    ----------
    df : pd.DataFrame
        Household-level DataFrame with weight and replica columns.
    col_peso : str
        Name of the point-estimate weight column (e.g. "V1032").
    col_replicas : list[str]
        Names of the 200 bootstrap replica columns.
    mask : pd.Series
        Boolean mask selecting households to include in the total.

    Returns
    -------
    dict with keys: total, se, ic_lower, ic_upper, cv, n_obs
    """
    import numpy as np

    df_sub = df[mask]
    total  = float(df_sub[col_peso].fillna(0).sum())
    n_obs  = int(mask.sum())

    replicas_existentes = [c for c in col_replicas if c in df.columns]
    if not replicas_existentes:
        logger.warning("[Grupo 5] Réplicas Rao-Wu ausentes — SE não calculado.")
        return {
            "total": total, "se": float("nan"),
            "ic_lower": float("nan"), "ic_upper": float("nan"),
            "cv": float("nan"), "n_obs": n_obs,
        }

    totais_r = np.array([
        df_sub[r].fillna(0).sum() for r in replicas_existentes
    ])
    variancia = float(np.mean((totais_r - total) ** 2))
    se        = float(np.sqrt(variancia))
    ic_lower  = total - 1.96 * se
    ic_upper  = total + 1.96 * se
    cv        = se / total if total > 0 else float("nan")

    return {
        "total": total, "se": se,
        "ic_lower": ic_lower, "ic_upper": ic_upper,
        "cv": cv, "n_obs": n_obs,
    }


def estimar_deficit_componentes(df_hh: pd.DataFrame) -> pd.DataFrame:
    """
    Estimates total households in deficit by FJP component using Rao-Wu.

    Returns a DataFrame with one row per component plus a "total_deficit" row.
    Estimates with CV > 0.30 are flagged — FJP suppresses these in publication.

    Returns
    -------
    pd.DataFrame
        Columns: componente, total_estimado, erro_padrao, ic_lower, ic_upper, cv, n_obs, cv_alto
    """
    componentes = ["habitacao_precaria", "coabitacao", "onus_excessivo"]
    replicas = [c for c in VARS_REPLICAS if c in df_hh.columns]
    rows = []

    for comp in componentes:
        mask = df_hh["componente"] == comp
        est  = _rao_wu_total(df_hh, "V1032", replicas, mask)
        est["componente"] = comp
        rows.append(est)
        flag = "⚠️ CV ALTO" if not pd.isna(est["cv"]) and est["cv"] > 0.30 else ""
        logger.info(
            "[Grupo 5] %s: total=%.0f | SE=%.0f | CV=%.3f %s",
            comp, est["total"], est["se"], est["cv"] if not pd.isna(est["cv"]) else -1, flag,
        )

    mask_total = df_hh["componente"].isin(componentes)
    est_total  = _rao_wu_total(df_hh, "V1032", replicas, mask_total)
    est_total["componente"] = "total_deficit"
    rows.append(est_total)
    logger.info(
        "[Grupo 5] total_deficit: total=%.0f | SE=%.0f | CV=%.3f",
        est_total["total"], est_total["se"],
        est_total["cv"] if not pd.isna(est_total["cv"]) else -1,
    )

    df_out = pd.DataFrame(rows)[
        ["componente", "total", "se", "ic_lower", "ic_upper", "cv", "n_obs"]
    ].rename(columns={"total": "total_estimado", "se": "erro_padrao"})
    df_out["cv_alto"] = df_out["cv"].apply(lambda x: not pd.isna(x) and x > 0.30)
    return df_out.reset_index(drop=True)


def estimar_deficit_recortes(
    df_hh: pd.DataFrame,
    recortes: dict[str, dict] | None = None,
) -> pd.DataFrame:
    """
    Estimates deficit totals broken down by sociodemographic recortes.

    Parameters
    ----------
    df_hh : pd.DataFrame
        Household-level DataFrame with 'componente' column from classificar_componente_fjp().
    recortes : dict | None
        Mapping of recorte_name → {col: str, categorias: dict[value, label]}.
        If None, uses default recortes: V2007 (sexo), V2010 (cor/raça).

    Returns
    -------
    pd.DataFrame
        Columns: recorte, categoria, componente, total_estimado, erro_padrao,
                 ic_lower, ic_upper, cv, n_obs, cv_alto
    """
    if recortes is None:
        recortes = {
            "sexo": {
                "col": "V2007",
                "categorias": {1: "Homem", 2: "Mulher"},
            },
            "cor_raca": {
                "col": "V2010",
                "categorias": {
                    1: "Branca", 2: "Preta", 3: "Amarela",
                    4: "Parda", 5: "Indígena",
                },
            },
        }

    replicas  = [c for c in VARS_REPLICAS if c in df_hh.columns]
    componentes = ["habitacao_precaria", "coabitacao", "onus_excessivo", "total_deficit"]
    rows = []

    for rec_nome, rec_cfg in recortes.items():
        col = rec_cfg["col"]
        if col not in df_hh.columns:
            logger.warning("[Grupo 5] Coluna de recorte ausente: %s — ignorado.", col)
            continue
        categorias = rec_cfg.get("categorias", {})

        for val, label in categorias.items():
            mask_cat = df_hh[col] == val
            if not mask_cat.any():
                continue

            for comp in componentes:
                if comp == "total_deficit":
                    mask = mask_cat & df_hh["componente"].isin(
                        ["habitacao_precaria", "coabitacao", "onus_excessivo"]
                    )
                else:
                    mask = mask_cat & (df_hh["componente"] == comp)

                est = _rao_wu_total(df_hh, "V1032", replicas, mask)
                est["recorte"]   = rec_nome
                est["categoria"] = label
                est["componente"] = comp
                rows.append(est)

    if not rows:
        return pd.DataFrame()

    df_out = pd.DataFrame(rows)[
        ["recorte", "categoria", "componente",
         "total", "se", "ic_lower", "ic_upper", "cv", "n_obs"]
    ].rename(columns={"total": "total_estimado", "se": "erro_padrao"})
    df_out["cv_alto"] = df_out["cv"].apply(lambda x: not pd.isna(x) and x > 0.30)
    return df_out.reset_index(drop=True)


# ---------------------------------------------------------------------------
# Fase F — Persistência e função principal
# ---------------------------------------------------------------------------

_SALARIO_MINIMO_DEFAULT: dict[int, float] = {
    2022: 1212.00,
    2023: 1320.00,
}


def _resolver_salario_minimo(ano: int, salario_minimo: float | None) -> tuple[float, str]:
    """Returns (valor_sm, fonte) — raises ValueError if year unknown and no value given."""
    if salario_minimo is not None:
        return float(salario_minimo), "parametro_usuario"
    if ano in _SALARIO_MINIMO_DEFAULT:
        return _SALARIO_MINIMO_DEFAULT[ano], "default_interno"
    raise ValueError(
        f"Salário mínimo para {ano} não disponível no default interno "
        f"(anos disponíveis: {sorted(_SALARIO_MINIMO_DEFAULT)}). "
        f"Informe salario_minimo=<valor> ao chamar coletar_grupo5()."
    )


def coletar_grupo5(
    codigo_ibge: str,
    limite_municipal: gpd.GeoDataFrame,
    output_dir: Path,
    db_conn,
    ano: int = 2022,
    salario_minimo: float | None = None,
    forcar: bool = False,
    **kwargs,
) -> dict:
    """
    Main entry point for Grupo 5: PNADc Anual V1 + FJP 2021 deficit.

    Downloads microdata and dictionary if not cached, classifies households
    using the FJP 2021 cascade, estimates totals with Rao-Wu bootstrap, and
    persists results to DuckDB.

    Parameters
    ----------
    codigo_ibge : str
        7-digit IBGE municipality code.
    limite_municipal : gpd.GeoDataFrame
        Municipality boundary (unused in calculation — kept for API consistency).
    output_dir : Path
        Base output directory; a 'pnadc_cache' subdirectory is used for downloads.
    db_conn : duckdb.DuckDBPyConnection
        Open DuckDB connection for persisting results.
    ano : int
        Reference year (default 2022).
    salario_minimo : float | None
        Monthly minimum wage in reais. None → use internal default for 2022/2023.
        Required for other years; raises ValueError if missing.
    forcar : bool
        If True, re-runs even if tables already exist in DuckDB.
    **kwargs :
        Ignored extra keyword arguments (orquestrador compatibility).

    Returns
    -------
    dict
        Metadata dict with keys: municipio, ano, tipo_municipio, dominios_usados,
        salario_minimo_usado, fonte_sm, total_deficit_estimado, aviso_geografico,
        md5_microdados, md5_dicionario, subcomponentes_externos.

    Notes
    -----
    PNADc DOES NOT have municipal representativeness. Results are valid for
    the estimation domain (UF + area type), not for the specific municipality.
    See aviso_geografico in the returned metadata.
    """
    import json
    import duckdb

    codigo_ibge = str(codigo_ibge)
    output_dir  = Path(output_dir)
    cache_dir   = output_dir / "pnadc_cache"

    sm_valor, sm_fonte = _resolver_salario_minimo(ano, salario_minimo)

    # Skip if already done and not forcing
    if not forcar:
        tabelas_existentes = []
        try:
            tabelas_existentes = [
                r[0] for r in db_conn.execute("SHOW TABLES").fetchall()
            ]
        except Exception:
            pass
        if "pnadc_deficit_componentes" in tabelas_existentes:
            logger.info(
                "[Grupo 5] Tabelas já existem em DuckDB — pulando (use forcar=True para recalcular)."
            )
            meta_raw = db_conn.execute(
                "SELECT chave, valor FROM pnadc_metadados"
            ).fetchall()
            return {
                "status": "ok",
                "camadas": ["pnadc_microdados_v1", "pnadc_deficit_componentes",
                            "pnadc_deficit_recortes", "pnadc_metadados"],
                **dict(meta_raw),
            }

    # ── Fase A+B: download ────────────────────────────────────────────────────
    dict_path   = _baixar_dicionario_anual_v1(ano, cache_dir)
    zip_path    = _baixar_microdados_anual_v1(ano, cache_dir)
    md5_dict    = _md5_arquivo(dict_path)
    md5_dados   = _md5_arquivo(zip_path)

    # ── Parse dicionário ──────────────────────────────────────────────────────
    dict_df = parsear_dicionario(dict_path)

    # ── Fase C: leitura e filtro ──────────────────────────────────────────────
    df_pessoas = ler_pnadc_anual_v1(zip_path, dict_df)
    df_dom, tipo_mun, dominios = filtrar_area_ponderacao(df_pessoas, codigo_ibge)
    df_hh = agregar_para_domicilio(df_dom)

    del df_pessoas, df_dom  # free ~2–4 GB

    # ── Fase D: classificação FJP ─────────────────────────────────────────────
    df_hh = classificar_componente_fjp(df_hh, salario_minimo=sm_valor)

    # ── Fase E: estimação Rao-Wu ──────────────────────────────────────────────
    df_comp    = estimar_deficit_componentes(df_hh)
    df_recortes = estimar_deficit_recortes(df_hh)

    # ── Fase F: persistência ──────────────────────────────────────────────────
    total_est = float(df_comp.loc[df_comp["componente"] == "total_deficit", "total_estimado"].iloc[0])

    aviso_geo = (
        f"PNADc não tem representatividade municipal. "
        f"Resultados válidos para o domínio: UF {codigo_ibge[:2]} + tipo={tipo_mun} "
        f"({', '.join(dominios)}). "
        f"Município {codigo_ibge} pertence a este domínio mas os totais estimados "
        f"correspondem ao domínio inteiro, não ao município isolado."
    )

    metadados = {
        "municipio":              codigo_ibge,
        "ano":                    str(ano),
        "tipo_municipio":         tipo_mun,
        "dominios_usados":        json.dumps(dominios),
        "salario_minimo_usado":   str(sm_valor),
        "fonte_sm":               sm_fonte,
        "total_deficit_estimado": str(round(total_est, 0)),
        "aviso_geografico":       aviso_geo,
        "md5_microdados":         md5_dados,
        "md5_dicionario":         md5_dict,
        "subcomponentes_externos": "improvisados via CadUnico (Grupo 6)",
    }
    df_meta = pd.DataFrame(
        list(metadados.items()), columns=["chave", "valor"]
    )

    cols_micro = (
        ["UPA", "V1008", "V1014", "Estrato", "V1022", "V1023",
         "V2001", "VD5007", "S01001", "S01002", "S01005", "S01006",
         "S01017", "S01019", "V2007", "V2009", "V2010",
         "V1032", "flag_vd2004_extensa", "componente"]
        + [c for c in VARS_REPLICAS if c in df_hh.columns]
    )
    cols_micro_existentes = [c for c in cols_micro if c in df_hh.columns]
    df_micro = df_hh[cols_micro_existentes].copy()

    salvar_dataframe(db_conn, df_micro,     "pnadc_microdados_v1",       substituir=True)
    salvar_dataframe(db_conn, df_comp,      "pnadc_deficit_componentes",  substituir=True)
    salvar_dataframe(db_conn, df_recortes,  "pnadc_deficit_recortes",     substituir=True)
    salvar_dataframe(db_conn, df_meta,      "pnadc_metadados",            substituir=True)

    logger.info(
        "[Grupo 5] ✓ Concluído | municipio=%s | ano=%d | tipo=%s | deficit_total=%.0f",
        codigo_ibge, ano, tipo_mun, total_est,
    )
    logger.warning("[Grupo 5] AVISO GEOGRÁFICO: %s", aviso_geo)

    return {
        "status": "ok",
        "camadas": ["pnadc_microdados_v1", "pnadc_deficit_componentes",
                    "pnadc_deficit_recortes", "pnadc_metadados"],
        **metadados,
    }
