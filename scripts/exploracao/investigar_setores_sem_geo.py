"""Investiga setores sem geometria no proxy_setor."""
import sys
from pathlib import Path
sys.stdout.reconfigure(encoding="utf-8")
sys.path.insert(0, str(Path(__file__).parents[2]))

from modulo_coleta.utils.db_utils import abrir_conexao

conn = abrir_conexao(Path("data/processed/al_arapiraca/al_arapiraca.duckdb"))

# Setores com geometria nula
sem_geo = conn.execute(
    "SELECT cod_setor, n_dom_total, proxy_carencias_setor FROM proxy_setor WHERE geometry IS NULL ORDER BY n_dom_total DESC LIMIT 10"
).df()
print("=== Top 10 setores sem geometria (por n_dom_total) ===")
print(sem_geo.to_string(index=False))

# Total de domicilios sem geometria
stats = conn.execute("""
    SELECT
        COUNT(*) AS n_setores,
        SUM(CASE WHEN n_dom_total > 0 THEN n_dom_total ELSE 0 END) AS dom_sem_geo
    FROM proxy_setor WHERE geometry IS NULL
""").df()
print(f"\nSetores sem geo: {stats['n_setores'][0]}, domicilios afetados: {stats['dom_sem_geo'][0]}")

# Verificar formato dos Cod_setor no censo vs CD_SETOR nos setores
cod_censo = conn.execute("SELECT DISTINCT LEFT(Cod_setor, 15) AS cs FROM censo_domicilio01").df()
cd_geo = conn.execute("SELECT DISTINCT LEFT(CD_SETOR, 15) AS cs FROM setores_censitarios").df()

so_censo = set(cod_censo["cs"]) - set(cd_geo["cs"])
so_geo = set(cd_geo["cs"]) - set(cod_censo["cs"])
print(f"\nSetores so no censo: {len(so_censo)}")
print(f"Setores so na geometria: {len(so_geo)}")

if so_censo:
    print("Exemplos so no censo:", list(so_censo)[:5])
if so_geo:
    print("Exemplos so na geometria:", list(so_geo)[:5])

# Verificar distribuicao por tipo de setor (ultimo char de CD_SETOR)
tipos = conn.execute(
    "SELECT RIGHT(CD_SETOR, 1) AS tipo, COUNT(*) AS n FROM setores_censitarios GROUP BY tipo ORDER BY n DESC"
).df()
print("\n=== Tipos de setor (sufixo CD_SETOR) ===")
print(tipos.to_string(index=False))

conn.close()
