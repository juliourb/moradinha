"""
Teste de grupo1_geometrias.py com o município 2701407 (Campo Alegre-AL).

Execução:
    C:/Users/julio/anaconda3/envs/moradinha/python.exe tests/teste_grupo1.py

Verificações:
    - 4 camadas salvas em data/raw/al_campo_alegre/geometria/
    - 4 tabelas no DuckDB
    - CRS EPSG:4674 em todas
    - Contagem de registros não nula
"""

import sys
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))

from modulo_coleta.utils.db_utils import abrir_conexao, listar_tabelas
from modulo_coleta.grupos.grupo1_geometrias import coletar_grupo1
import geopandas as gpd

CODIGO_IBGE = "2701407"
SIGLA_MUN   = "al_campo_alegre"
OUTPUT_DIR  = ROOT / "data" / "raw"  / SIGLA_MUN / "geometria"
DB_PATH     = ROOT / "data" / "processed" / SIGLA_MUN / f"{SIGLA_MUN}.duckdb"

print("=" * 60)
print(f"TESTE grupo1 — {SIGLA_MUN} ({CODIGO_IBGE})")
print("=" * 60)

conn = abrir_conexao(DB_PATH)

resultado = coletar_grupo1(
    codigo_ibge=CODIGO_IBGE,
    limite_municipal=None,
    output_dir=OUTPUT_DIR,
    db_conn=conn,
)

print("\nResultado:", resultado)
assert resultado["status"] == "ok", f"Esperado ok, obtido: {resultado}"
assert len(resultado["camadas"]) == 4, f"Esperadas 4 camadas, obtidas: {resultado['camadas']}"

print("\nTabelas no banco:")
tabelas = listar_tabelas(conn)
print(" ", tabelas)
for t in ["limite_municipal", "setores_censitarios", "grade_estatistica", "areas_ponderacao"]:
    assert t in tabelas, f"Tabela '{t}' ausente no DuckDB"
    n = conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
    print(f"  {t}: {n} registros")
    assert n > 0

print("\nArquivos salvos:")
for gpkg in OUTPUT_DIR.glob("*.gpkg"):
    gdf = gpd.read_file(gpkg)
    print(f"  {gpkg.name}: {len(gdf)} registros | CRS: {gdf.crs}")
    assert str(gdf.crs) == "EPSG:4674", f"CRS incorreto em {gpkg.name}: {gdf.crs}"

conn.close()

print("\n" + "=" * 60)
print("TODOS OS TESTES PASSARAM")
print("=" * 60)
