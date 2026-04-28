"""Re-executa apenas a parte H3 da Etapa 2 para corrigir n_domicilios_grade e salvar mapeamento."""
import sys
import logging
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")
logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
sys.path.insert(0, str(Path(__file__).parents[2]))

from modulo_coleta.utils.db_utils import abrir_conexao
from modulo_estimacao.etapas_t0.etapa2_covariaveis_t0 import extrair_covariaveis_h3_t0

DB = Path("data/processed/al_arapiraca/al_arapiraca.duckdb")
conn = abrir_conexao(DB)

r = extrair_covariaveis_h3_t0("2700300", 2022, 8, conn, salvar=True)
print("\n=== Resultado Etapa 2-H3 ===")
for k, v in r.items():
    print(f"  {k}: {v}")

# Verifica n_domicilios_grade
print("\n=== covariaveis_h3_t0 — n_domicilios_grade ===")
stats = conn.execute("""
    SELECT
        COUNT(*) AS n_hex,
        SUM(n_domicilios_grade) AS total_dom,
        COUNT(n_domicilios_grade) AS n_preenchidos,
        ROUND(AVG(n_domicilios_grade), 1) AS media_dom
    FROM covariaveis_h3_t0
""").df()
print(stats.to_string(index=False))

print("\n=== mapeamento_h3_setor_t0 — amostra ===")
tab = conn.execute("SHOW TABLES").fetchall()
if any("mapeamento_h3_setor_t0" in t for t in tab):
    m = conn.execute("""
        SELECT cod_setor, h3_index, ROUND(peso_area, 4) AS peso_area
        FROM mapeamento_h3_setor_t0
        ORDER BY cod_setor
        LIMIT 10
    """).df()
    print(m.to_string(index=False))
    total_pares = conn.execute("SELECT COUNT(*) FROM mapeamento_h3_setor_t0").fetchone()[0]
    print(f"\n  Total de pares: {total_pares}")
else:
    print("  mapeamento_h3_setor_t0 NAO encontrado")

conn.close()
print("\nRe-teste Etapa 2-H3 concluído.")
