"""
Teste de db_utils.py com o município de Arapiraca-AL (2701407).

Execução:
    C:/Users/julio/anaconda3/envs/moradinha/python.exe tests/teste_db_utils.py

O que é testado:
    1. abrir_conexao()      → cria o arquivo .duckdb e ativa extensão spatial
    2. salvar_dataframe()   → tabela simples sem geometria
    3. salvar_geodataframe() → tabela com geometria (polígono simples)
    4. listar_tabelas()     → confirma que as duas tabelas existem no banco
    5. Idempotência         → executar de novo não duplica dados
"""

import sys
from pathlib import Path

# Permite rodar de qualquer pasta
ROOT = Path(__file__).resolve().parent.parent.parent  # moradinha/
sys.path.insert(0, str(ROOT))

import geopandas as gpd
import pandas as pd
from shapely.geometry import Point, Polygon

from modulo_coleta.utils.db_utils import (
    abrir_conexao,
    listar_tabelas,
    salvar_dataframe,
    salvar_geodataframe,
)

# ---------------------------------------------------------------------------
# Configurações do teste
# ---------------------------------------------------------------------------
CODIGO_IBGE = "2701407"      # Arapiraca - AL
SIGLA_MUN   = "al_arapiraca"
DB_PATH     = ROOT / "data" / "processed" / SIGLA_MUN / f"{SIGLA_MUN}.duckdb"

# ---------------------------------------------------------------------------
# Dado sintético mínimo — não depende de download externo
# ---------------------------------------------------------------------------
# DataFrame simples (sem geometria)
df_teste = pd.DataFrame({
    "codigo_ibge": [CODIGO_IBGE],
    "nome":        ["Arapiraca"],
    "uf":          ["AL"],
    "populacao":   [230_417],
})

# GeoDataFrame com um polígono fictício em EPSG:4674
poligono_ficticio = Polygon([
    (-36.70, -9.77), (-36.55, -9.77),
    (-36.55, -9.60), (-36.70, -9.60),
    (-36.70, -9.77),
])
gdf_teste = gpd.GeoDataFrame(
    {"nome": ["limite_ficticio"], "area_km2": [100.0]},
    geometry=[poligono_ficticio],
    crs="EPSG:4674",
)

# ---------------------------------------------------------------------------
# Testes
# ---------------------------------------------------------------------------
print("=" * 60)
print(f"TESTE db_utils.py — Município: {SIGLA_MUN} ({CODIGO_IBGE})")
print("=" * 60)

# 1. Abrir conexão
print("\n[1] abrir_conexao()")
conn = abrir_conexao(DB_PATH)
print(f"    OK — banco criado em: {DB_PATH}")

# 2. salvar_dataframe
print("\n[2] salvar_dataframe() — tabela 'info_municipio'")
salvar_dataframe(conn, df_teste, "info_municipio")
resultado = conn.execute("SELECT * FROM info_municipio").fetchdf()
print(resultado.to_string(index=False))

# 3. salvar_geodataframe
print("\n[3] salvar_geodataframe() — tabela 'limite_municipal_teste'")
salvar_geodataframe(conn, gdf_teste, "limite_municipal_teste")
resultado_geo = conn.execute(
    "SELECT nome, area_km2 FROM limite_municipal_teste"
).fetchdf()
print(resultado_geo.to_string(index=False))

# 4. listar_tabelas
print("\n[4] listar_tabelas()")
tabelas = listar_tabelas(conn)
print(f"    Tabelas no banco: {tabelas}")
assert "info_municipio" in tabelas,        "ERRO: tabela info_municipio não encontrada"
assert "limite_municipal_teste" in tabelas, "ERRO: tabela limite_municipal_teste não encontrada"
print("    ASSERT OK — ambas as tabelas encontradas")

# 5. Idempotência — rodar de novo não deve duplicar
print("\n[5] Idempotência — re-salvar e conferir contagem")
salvar_dataframe(conn, df_teste, "info_municipio")
n = conn.execute("SELECT COUNT(*) FROM info_municipio").fetchone()[0]
assert n == 1, f"ERRO: esperado 1 registro, encontrado {n}"
print(f"    OK — {n} registro (sem duplicata)")

conn.close()

print("\n" + "=" * 60)
print("TODOS OS TESTES PASSARAM")
print(f"Arquivo DuckDB: {DB_PATH}")
print("=" * 60)
