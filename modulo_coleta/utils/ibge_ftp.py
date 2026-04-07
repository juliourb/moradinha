"""
utils/ibge_ftp.py — Helpers de download FTP/HTTPS do IBGE e descompressão.

Refatorado a partir de template_dados_IBGE_por_municipio.ipynb:
    - obter_codigo_uf / obter_sigla_uf    → derivados do codigo_ibge
    - importar_setores_censitarios         → agora salva em disco com idempotência
    - obter_enderecos_cnefe                → refatorado de obter_endereços()
    - baixar_faces_logradouros             → refatorado de baixar_faces_logradouros_municipio()

Funções públicas
----------------
obter_codigo_uf(codigo_ibge)   → str   ex: "27"
obter_sigla_uf(codigo_ibge)    → str   ex: "AL"
baixar_arquivo(url, destino)   → Path  download com tqdm + idempotência
descompactar_zip(zip_path, destino_dir) → list[Path]
buscar_zip_no_ftp(url_base, prefixo)    → str  URL do ZIP encontrado na listagem FTP
"""

from __future__ import annotations

import hashlib
import logging
import re
import tempfile
from io import BytesIO
from pathlib import Path
from urllib.request import urlopen, urlretrieve
from zipfile import ZipFile

import geopandas as gpd
import pandas as pd
import requests
from tqdm import tqdm

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Tabela UF — derivada do código IBGE (2 primeiros dígitos = código UF)
# ---------------------------------------------------------------------------
_CODIGO_PARA_SIGLA: dict[str, str] = {
    "11": "RO", "12": "AC", "13": "AM", "14": "RR", "15": "PA",
    "16": "AP", "17": "TO", "21": "MA", "22": "PI", "23": "CE",
    "24": "RN", "25": "PB", "26": "PE", "27": "AL", "28": "SE",
    "29": "BA", "31": "MG", "32": "ES", "33": "RJ", "35": "SP",
    "41": "PR", "42": "SC", "43": "RS", "50": "MS", "51": "MT",
    "52": "GO", "53": "DF",
}


def obter_codigo_uf(codigo_ibge: str) -> str:
    """
    Extrai o código numérico de 2 dígitos da UF a partir do código IBGE municipal.

    O código IBGE de município tem 7 dígitos; os 2 primeiros identificam a UF.

    Parâmetros
    ----------
    codigo_ibge : str
        Código IBGE do município com 7 dígitos. Ex: "2701407" → "27" (AL).

    Retorna
    -------
    str
        Código numérico da UF. Ex: "27".

    Levanta
    -------
    ValueError
        Se o código não corresponder a nenhuma UF conhecida.
    """
    cod = str(codigo_ibge)[:2]
    if cod not in _CODIGO_PARA_SIGLA:
        raise ValueError(f"Código UF '{cod}' não reconhecido (codigo_ibge={codigo_ibge}).")
    return cod


def obter_sigla_uf(codigo_ibge: str) -> str:
    """
    Obtém a sigla da UF (ex: "AL") a partir do código IBGE municipal.

    Parâmetros
    ----------
    codigo_ibge : str
        Código IBGE do município com 7 dígitos. Ex: "2701407" → "AL".

    Retorna
    -------
    str
        Sigla da UF em maiúsculas. Ex: "AL".
    """
    return _CODIGO_PARA_SIGLA[obter_codigo_uf(codigo_ibge)]


# ---------------------------------------------------------------------------
# Download com idempotência e barra de progresso
# ---------------------------------------------------------------------------

def md5_arquivo(path: Path) -> str:
    """Calcula o hash MD5 de um arquivo em disco."""
    h = hashlib.md5()
    with open(path, "rb") as f:
        for bloco in iter(lambda: f.read(65536), b""):
            h.update(bloco)
    return h.hexdigest()


def baixar_arquivo(
    url: str,
    destino: Path,
    forcar: bool = False,
) -> Path:
    """
    Baixa um arquivo para 'destino' com barra de progresso tqdm.

    Idempotente: se o arquivo já existe e forcar=False, o download é pulado.

    Parâmetros
    ----------
    url : str
        URL do arquivo a baixar.
    destino : Path
        Caminho completo onde o arquivo será salvo.
    forcar : bool
        Se True, baixa mesmo que o arquivo já exista. Padrão: False.

    Retorna
    -------
    Path
        Caminho do arquivo baixado (mesmo que 'destino').

    Levanta
    -------
    requests.HTTPError
        Se a requisição retornar status HTTP != 2xx.
    """
    destino = Path(destino)
    destino.parent.mkdir(parents=True, exist_ok=True)

    if destino.exists() and not forcar:
        logger.info("Arquivo já existe (pulando download): %s", destino)
        return destino

    logger.info("Baixando: %s → %s", url, destino)
    resposta = requests.get(url, stream=True, timeout=120)
    resposta.raise_for_status()

    tamanho_total = int(resposta.headers.get("content-length", 0))
    with (
        open(destino, "wb") as f,
        tqdm(
            total=tamanho_total,
            unit="B",
            unit_scale=True,
            unit_divisor=1024,
            desc=destino.name,
        ) as barra,
    ):
        for bloco in resposta.iter_content(chunk_size=65536):
            f.write(bloco)
            barra.update(len(bloco))

    logger.info("Download concluído: %s (MD5: %s)", destino, md5_arquivo(destino))
    return destino


def descompactar_zip(zip_path: Path, destino_dir: Path) -> list[Path]:
    """
    Extrai todos os arquivos de um ZIP para 'destino_dir'.

    Parâmetros
    ----------
    zip_path : Path
        Caminho do arquivo .zip.
    destino_dir : Path
        Pasta onde os arquivos serão extraídos.

    Retorna
    -------
    list[Path]
        Lista com os caminhos dos arquivos extraídos.
    """
    destino_dir = Path(destino_dir)
    destino_dir.mkdir(parents=True, exist_ok=True)

    with ZipFile(zip_path) as z:
        z.extractall(destino_dir)
        extraidos = [destino_dir / nome for nome in z.namelist()]

    logger.info("%d arquivo(s) extraído(s) em %s", len(extraidos), destino_dir)
    return extraidos


def buscar_zip_no_ftp(url_base: str, prefixo: str) -> str:
    """
    Busca dinamicamente a URL de um arquivo ZIP na listagem de um diretório FTP/HTTP.

    Necessário para o Censo 2022 agregado, cujo nome de arquivo varia por UF
    e data de publicação (ex: 'AL_20231030.zip').

    Parâmetros
    ----------
    url_base : str
        URL do diretório FTP/HTTP a listar. Ex:
        "https://ftp.ibge.gov.br/Censos/.../Agregados_por_Setores_Censitarios/"
    prefixo : str
        Prefixo do arquivo a encontrar. Ex: "AL_" (para localizar AL_20231030.zip).

    Retorna
    -------
    str
        URL completa do primeiro arquivo ZIP cujo nome começa com 'prefixo'.

    Levanta
    -------
    FileNotFoundError
        Se nenhum arquivo com o prefixo for encontrado.
    """
    resposta = requests.get(url_base, timeout=30)
    resposta.raise_for_status()

    # Extrai nomes de arquivos .zip da listagem HTML do FTP IBGE
    padrao = re.compile(rf'href="({re.escape(prefixo)}[^"]*\.zip)"', re.IGNORECASE)
    encontrados = padrao.findall(resposta.text)

    if not encontrados:
        raise FileNotFoundError(
            f"Nenhum ZIP com prefixo '{prefixo}' encontrado em: {url_base}"
        )

    nome_zip = encontrados[0]
    url_completa = url_base.rstrip("/") + "/" + nome_zip
    logger.info("ZIP encontrado: %s", url_completa)
    return url_completa


# ---------------------------------------------------------------------------
# Funções de download de camadas IBGE específicas
# (refatoradas do template_dados_IBGE_por_municipio.ipynb)
# ---------------------------------------------------------------------------

def baixar_setores_censitarios(
    codigo_ibge: str,
    output_dir: Path,
    forcar: bool = False,
) -> Path:
    """
    Baixa o GeoPackage de setores censitários 2022 (preliminar) para a UF
    do município e salva em disco.

    Refatorado de: importar_setores_censitarios(sigla_uf) do template notebook.
    Diferença: salva em disco com idempotência, não retorna GeoDataFrame direto.

    Fonte:
        geoftp.ibge.gov.br/.../censo_2022_preliminar/setores/gpkg/UF/{SIGLA}/
        {SIGLA}_Malha_Preliminar_2022.gpkg

    Parâmetros
    ----------
    codigo_ibge : str
        Código IBGE de 7 dígitos do município.
    output_dir : Path
        Pasta onde o .gpkg será salvo (ex: data/raw/al_arapiraca/geometria/).
    forcar : bool
        Se True, rebaixa mesmo que o arquivo já exista.

    Retorna
    -------
    Path
        Caminho do arquivo .gpkg baixado.
    """
    sigla = obter_sigla_uf(codigo_ibge)
    nome_arquivo = f"{sigla}_Malha_Preliminar_2022.gpkg"
    url = (
        "https://geoftp.ibge.gov.br/organizacao_do_territorio"
        "/malhas_territoriais/malhas_de_setores_censitarios__divisoes_intramunicipais"
        f"/censo_2022_preliminar/setores/gpkg/UF/{sigla}/{nome_arquivo}"
    )
    destino = Path(output_dir) / nome_arquivo
    return baixar_arquivo(url, destino, forcar=forcar)


def baixar_cnefe(
    codigo_ibge: str,
    output_dir: Path,
    forcar: bool = False,
) -> Path:
    """
    Baixa o ZIP com endereços CNEFE do Censo 2022 para a UF do município.

    Refatorado de: obter_endereços(sigla_uf) do template notebook.
    Diferença: salva o ZIP em disco com idempotência; o parse do CSV é feito
    no grupo3_logradouros.py.

    Fonte:
        ftp.ibge.gov.br/.../Cadastro_Nacional_de_Enderecos.../UF/{cod}_{sigla}.zip

    Parâmetros
    ----------
    codigo_ibge : str
        Código IBGE de 7 dígitos do município.
    output_dir : Path
        Pasta onde o ZIP será salvo.
    forcar : bool
        Se True, rebaixa mesmo que o arquivo já exista.

    Retorna
    -------
    Path
        Caminho do arquivo .zip baixado.
    """
    sigla = obter_sigla_uf(codigo_ibge)
    cod_uf = obter_codigo_uf(codigo_ibge)
    nome_zip = f"{cod_uf}_{sigla}.zip"
    url = (
        "https://ftp.ibge.gov.br/Cadastro_Nacional_de_Enderecos_para_Fins_Estatisticos"
        f"/Censo_Demografico_2022/Coordenadas_enderecos/UF/{nome_zip}"
    )
    destino = Path(output_dir) / nome_zip
    return baixar_arquivo(url, destino, forcar=forcar)


def baixar_faces_logradouros(
    codigo_ibge: str,
    output_dir: Path,
    forcar: bool = False,
) -> Path:
    """
    Baixa o ZIP com faces de logradouro IBGE (versão 2021) para a UF do município.

    Refatorado de: baixar_faces_logradouros_municipio(codigo_municipio, sigla_uf)
    do template notebook.
    Diferença: salva em disco com idempotência; o filtro por CD_MUN é feito
    no grupo3_logradouros.py.

    Fonte:
        geoftp.ibge.gov.br/.../base_de_faces_de_logradouros_versao_2021/
        {sigla_lower}/{sigla_lower}_faces_de_logradouros_2021.zip

    Parâmetros
    ----------
    codigo_ibge : str
        Código IBGE de 7 dígitos do município.
    output_dir : Path
        Pasta onde o ZIP será salvo.
    forcar : bool
        Se True, rebaixa mesmo que o arquivo já exista.

    Retorna
    -------
    Path
        Caminho do arquivo .zip baixado.
    """
    sigla = obter_sigla_uf(codigo_ibge)  # maiúsculo: "AL"
    nome_zip = f"{sigla}_faces_de_logradouros_2022_shp.zip"
    url = (
        "https://geoftp.ibge.gov.br/recortes_para_fins_estatisticos"
        "/malha_de_setores_censitarios/censo_2022"
        f"/base_de_faces_de_logradouros_versao_2022_censo_demografico/shp/{nome_zip}"
    )
    destino = Path(output_dir) / nome_zip
    return baixar_arquivo(url, destino, forcar=forcar)


_URL_BASE_CENSO_AGREGADO = (
    "https://ftp.ibge.gov.br/Censos/Censo_Demografico_2022"
    "/Agregados_por_Setores_Censitarios/Agregados_por_Setor_csv/"
)

# Prefixos conhecidos de cada tipo de arquivo (arquivos nacionais BR)
_PREFIXO_CENSO: dict[str, str] = {
    "basico":      "Agregados_por_setores_basico_BR",
    "domicilio1":  "Agregados_por_setores_caracteristicas_domicilio1_BR",
    "domicilio2":  "Agregados_por_setores_caracteristicas_domicilio2_BR",
    "domicilio3":  "Agregados_por_setores_caracteristicas_domicilio3_BR",
}


def baixar_censo_agregado(
    tipo: str,
    output_dir: Path,
    forcar: bool = False,
) -> Path:
    """
    Baixa um dos ZIPs de Agregados por Setores Censitários do Censo 2022.

    Os arquivos são NACIONAIS (Brasil inteiro). O filtro por município
    é feito depois da extração, em grupo2_censo.py.

    Tipos disponíveis
    -----------------
    "basico"     → Agregados_por_setores_basico_BR*.zip
    "domicilio1" → características físicas do domicílio (V00001–V00089)
    "domicilio2" → acesso a serviços — água, esgoto, energia (V00090–V00495)
    "domicilio3" → características adicionais do domicílio

    Fonte:
        ftp.ibge.gov.br/Censos/Censo_Demografico_2022/
        Agregados_por_Setores_Censitarios/Agregados_por_Setor_csv/

    Parâmetros
    ----------
    tipo : str
        Um de: "basico", "domicilio1", "domicilio2", "domicilio3".
    output_dir : Path
        Pasta onde o ZIP será salvo.
    forcar : bool
        Se True, rebaixa mesmo que o arquivo já exista.

    Retorna
    -------
    Path
        Caminho do arquivo .zip baixado.

    Levanta
    -------
    ValueError
        Se 'tipo' não for reconhecido.
    """
    if tipo not in _PREFIXO_CENSO:
        raise ValueError(
            f"Tipo '{tipo}' inválido. Use um de: {list(_PREFIXO_CENSO)}"
        )

    prefixo = _PREFIXO_CENSO[tipo]
    output_dir = Path(output_dir)

    # Idempotência: verifica se já existe antes de listar o FTP
    existentes = list(output_dir.glob(f"{prefixo}*.zip"))
    if existentes and not forcar:
        logger.info("ZIP Censo '%s' ja existe (pulando): %s", tipo, existentes[0])
        return existentes[0]

    url_zip = buscar_zip_no_ftp(_URL_BASE_CENSO_AGREGADO, prefixo=prefixo)
    nome_zip = url_zip.split("/")[-1]
    destino = output_dir / nome_zip
    return baixar_arquivo(url_zip, destino, forcar=forcar)
