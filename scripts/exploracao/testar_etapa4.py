"""Testa a Etapa 4 (predição H3) com o modelo RF."""
import sys
import logging
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")
logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
sys.path.insert(0, str(Path(__file__).parents[2]))

from modulo_coleta.utils.db_utils import abrir_conexao
from modulo_estimacao.etapas_t0.etapa4_predicao_h3_t0 import predizer_h3_t0

DB = Path("data/processed/al_arapiraca/al_arapiraca.duckdb")
conn = abrir_conexao(DB)

r = predizer_h3_t0("2700300", 2022, 8, conn, output_dir=Path("data/processed/al_arapiraca"), modelo="rf")
print("\n=== Resultado Etapa 4 ===")
for k, v in r.items():
    print(f"  {k}: {v}")

if r["status"] == "ok":
    df = conn.execute("""
        SELECT h3_index, proxy_predito, proxy_ic_lower, proxy_ic_upper,
               deficit_estimado, n_domicilios_grade, n_imputed
        FROM deficit_predito_h3_t0
        ORDER BY deficit_estimado DESC NULLS LAST
        LIMIT 8
    """).df()
    print("\n=== Top 8 hexágonos por deficit_estimado ===")
    print(df.to_string(index=False))

    stats = conn.execute("""
        SELECT
            COUNT(*) AS n_hex,
            ROUND(AVG(proxy_predito), 4) AS proxy_medio,
            ROUND(MIN(proxy_predito), 4) AS proxy_min,
            ROUND(MAX(proxy_predito), 4) AS proxy_max,
            ROUND(SUM(deficit_estimado), 0) AS deficit_total,
            SUM(n_imputed > 0) AS n_imputed_hex
        FROM deficit_predito_h3_t0
    """).df()
    print("\n=== Estatisticas globais ===")
    print(stats.to_string(index=False))

conn.close()
print("\nTeste Etapa 4 concluido.")
