"""
Teste de ibge_ftp.py com o município de Arapiraca-AL (2701407).

Execução:
    C:/Users/julio/anaconda3/envs/moradinha/python.exe tests/teste_ibge_ftp.py

O que é testado:
    1. obter_codigo_uf()  → "27"
    2. obter_sigla_uf()   → "AL"
    3. buscar_zip_no_ftp() → encontra ZIP do Censo agregado AL no FTP IBGE
    4. baixar_setores_censitarios() → baixa .gpkg de setores da UF AL
    5. Filtro por CD_MUN  → confirma que setores de 2701407 estão no arquivo
    6. Idempotência       → segunda chamada pula o download
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent  # moradinha/
sys.path.insert(0, str(ROOT))

from modulo_coleta.utils.ibge_ftp import (
    baixar_setores_censitarios,
    buscar_zip_no_ftp,
    obter_codigo_uf,
    obter_sigla_uf,
)
import geopandas as gpd

CODIGO_IBGE = "2701407"   # Arapiraca - AL
OUTPUT_DIR  = ROOT / "data" / "raw" / "al_arapiraca" / "geometria"

print("=" * 60)
print(f"TESTE ibge_ftp.py — Município: Arapiraca ({CODIGO_IBGE})")
print("=" * 60)

# 1. obter_codigo_uf
print("\n[1] obter_codigo_uf()")
cod_uf = obter_codigo_uf(CODIGO_IBGE)
assert cod_uf == "27", f"Esperado '27', obtido '{cod_uf}'"
print(f"    OK → '{cod_uf}'")

# 2. obter_sigla_uf
print("\n[2] obter_sigla_uf()")
sigla = obter_sigla_uf(CODIGO_IBGE)
assert sigla == "AL", f"Esperado 'AL', obtido '{sigla}'"
print(f"    OK → '{sigla}'")

# 3. buscar_zip_no_ftp
print("\n[3] buscar_zip_no_ftp() — Censo agregado AL")
url_base_censo = (
    "https://ftp.ibge.gov.br/Censos/Censo_Demografico_2022"
    "/Resultados_do_Universo/Agregados_por_Setores_Censitarios/"
)
url_zip = buscar_zip_no_ftp(url_base_censo, prefixo="AL_")
assert "AL_" in url_zip and url_zip.endswith(".zip"), f"URL inesperada: {url_zip}"
print(f"    OK → {url_zip}")

# 4. baixar_setores_censitarios
print(f"\n[4] baixar_setores_censitarios() → {OUTPUT_DIR}")
print("    (pode demorar alguns minutos — arquivo da UF inteira)")
gpkg_path = baixar_setores_censitarios(CODIGO_IBGE, OUTPUT_DIR)
assert gpkg_path.exists(), f"Arquivo não encontrado: {gpkg_path}"
print(f"    OK → {gpkg_path} ({gpkg_path.stat().st_size / 1e6:.1f} MB)")

# 5. Filtro por CD_MUN
print("\n[5] Filtro por CD_MUN = '2701407'")
setores = gpd.read_file(gpkg_path)
setores_mun = setores[setores["CD_MUN"] == CODIGO_IBGE]
assert not setores_mun.empty, "Nenhum setor encontrado para Arapiraca"
print(f"    OK → {len(setores_mun)} setores encontrados")
print(f"    CRS: {setores_mun.crs}")
print(f"    Colunas: {list(setores_mun.columns)}")

# 6. Idempotência
print("\n[6] Idempotência — segunda chamada deve pular download")
gpkg_path2 = baixar_setores_censitarios(CODIGO_IBGE, OUTPUT_DIR)
assert gpkg_path2 == gpkg_path
print("    OK → download pulado (arquivo já existe)")

print("\n" + "=" * 60)
print("TODOS OS TESTES PASSARAM")
print("=" * 60)
