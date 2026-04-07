# moradinha/modulo_coleta/__init__.py
from .dependencias import verificar_e_instalar
verificar_e_instalar()

from .orquestrador import coletar_municipio

__all__ = ["coletar_municipio"]
