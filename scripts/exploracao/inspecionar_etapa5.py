"""Inspeciona o DuckDB para entender o que a Etapa 5 pode usar."""
import sys
import logging
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")
logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
sys.path.insert(0, str(Path(__file__).parents[2]))

from modulo_coleta.utils.db_utils import abrir_conexao

DB = Path("data/processed/al_arapiraca/al_arapiraca.duckdb")
conn = abrir_conexao(DB)

print("=== Tabelas no DuckDB ===")
tabelas = conn.execute("SHOW TABLES").fetchall()
for t in sorted(tabelas):
    print(f"  {t[0]}")

print("\n=== proxy_setor — schema e amostra ===")
cols = conn.execute("DESCRIBE proxy_setor").df()
print(cols[["column_name", "column_type"]].to_string(index=False))

print("\n=== proxy_setor — estatísticas de proxy_carencias_setor ===")
stats = conn.execute("""
    SELECT
        COUNT(*) AS n_setores,
        ROUND(AVG(proxy_carencias_setor), 4) AS proxy_medio,
        ROUND(SUM(n_dom_total), 0) AS total_dom,
        ROUND(SUM(n_dom_total * proxy_carencias_setor), 0) AS deficit_ponderado_total
    FROM proxy_setor
""").df()
print(stats.to_string(index=False))

print("\n=== deficit_predito_h3_t0 — schema ===")
cols4 = conn.execute("DESCRIBE deficit_predito_h3_t0").df()
print(cols4[["column_name", "column_type"]].to_string(index=False))

print("\n=== deficit_predito_h3_t0 — totais ===")
stats4 = conn.execute("""
    SELECT
        COUNT(*) AS n_hex,
        ROUND(SUM(deficit_estimado), 0) AS deficit_total,
        ROUND(AVG(proxy_predito), 4) AS proxy_medio,
        SUM(n_domicilios_grade) AS total_dom_grade
    FROM deficit_predito_h3_t0
""").df()
print(stats4.to_string(index=False))

# Verifica pnadc
print("\n=== Tabelas relacionadas a PNADc ===")
pnadc_tabs = [t[0] for t in tabelas if "pnadc" in t[0].lower()]
if pnadc_tabs:
    for tab in pnadc_tabs:
        print(f"\n  --- {tab} ---")
        desc = conn.execute(f"DESCRIBE {tab}").df()
        print(desc[["column_name", "column_type"]].to_string(index=False))
        sample = conn.execute(f"SELECT * FROM {tab} LIMIT 3").df()
        print(sample.to_string(index=False))
else:
    print("  Nenhuma tabela PNADc encontrada")

# Verifica covariaveis_h3_t0 — tem cod_setor?
print("\n=== covariaveis_h3_t0 — colunas disponíveis ===")
cols_h3 = conn.execute("DESCRIBE covariaveis_h3_t0").df()
print(cols_h3[["column_name", "column_type"]].to_string(index=False))

conn.close()
print("\nInspeção concluída.")
