"""
dependencias.py — Verificação e instalação automática de dependências.

Invocado pelo orquestrador antes de qualquer coleta. Lê o requirements.txt
da raiz do projeto, checa quais pacotes estão ausentes e instala via pip.

Mapeamento import → pacote pip quando os nomes diferem:
    rasterstats → rasterstats   (mesmo nome, mas import é rasterstats)
    PIL         → Pillow        (não usado diretamente, mas dependência indireta)
"""

from __future__ import annotations

import importlib.util
import logging
import subprocess
import sys
from pathlib import Path

logger = logging.getLogger(__name__)

# Mapeamento: nome do módulo Python → nome do pacote pip
# Necessário apenas quando os nomes diferem.
_IMPORT_PARA_PIP: dict[str, str] = {
    "sklearn": "scikit-learn",
    "cv2": "opencv-python",
    "PIL": "Pillow",
}


def _ler_requirements() -> list[str]:
    """Lê requirements.txt da raiz do projeto e retorna lista de pacotes."""
    raiz = Path(__file__).resolve().parent.parent
    req_path = raiz / "requirements.txt"
    if not req_path.exists():
        logger.warning("requirements.txt não encontrado em: %s", req_path)
        return []
    linhas = req_path.read_text(encoding="utf-8").splitlines()
    return [l.strip() for l in linhas if l.strip() and not l.startswith("#")]


def _esta_instalado(nome_import: str) -> bool:
    """Verifica se um pacote pode ser importado."""
    return importlib.util.find_spec(nome_import) is not None


def _nome_pip(pacote: str) -> str:
    """Resolve o nome pip a partir do nome de importação."""
    return _IMPORT_PARA_PIP.get(pacote, pacote)


def verificar_e_instalar() -> None:
    """
    Verifica se todos os pacotes do requirements.txt estão instalados.
    Instala via pip os que estiverem faltando.

    Levanta RuntimeError se alguma instalação falhar.
    """
    pacotes = _ler_requirements()
    if not pacotes:
        return

    ausentes = [p for p in pacotes if not _esta_instalado(p)]

    if not ausentes:
        logger.debug("Todas as dependências já estão instaladas.")
        return

    logger.info(
        "Dependências ausentes: %s. Instalando via pip...",
        ", ".join(ausentes),
    )

    pacotes_pip = [_nome_pip(p) for p in ausentes]
    cmd = [sys.executable, "-m", "pip", "install", "--quiet"] + pacotes_pip

    resultado = subprocess.run(cmd, capture_output=True, text=True)

    if resultado.returncode != 0:
        raise RuntimeError(
            f"Falha ao instalar dependências via pip.\n"
            f"Pacotes: {pacotes_pip}\n"
            f"Erro:\n{resultado.stderr}"
        )

    instalados = []
    falhos = []
    for pacote in ausentes:
        if _esta_instalado(pacote):
            instalados.append(pacote)
        else:
            falhos.append(pacote)

    if instalados:
        logger.info("Instalados com sucesso: %s", ", ".join(instalados))
    if falhos:
        raise RuntimeError(
            f"Pacotes instalados pelo pip mas ainda não importáveis: {falhos}. "
            "Pode ser necessário reiniciar o kernel/processo."
        )
