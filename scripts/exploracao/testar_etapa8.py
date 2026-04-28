"""Testa a Etapa 8 (predição H3 t1 — Arapiraca, t0=2022, t1=2024)."""
import sys
import logging
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")
logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
sys.path.insert(0, str(Path(__file__).parents[2]))

from modulo_coleta.utils.db_utils import abrir_conexao
from modulo_estimacao.etapas_t1.etapa8_predicao_h3_t1 import predizer_h3_t1

DB = Path("data/processed/al_arapiraca/al_arapiraca.duckdb")
conn = abrir_conexao(DB)

r = predizer_h3_t1(
    "2700300", ano_t0=2022, ano_t1=2024,
    resolucao_h3=8, db_conn=conn, usar_ancora_pnadc=True,
)
print("\n=== Resultado Etapa 8 ===")
for k, v in r.items():
    print(f"  {k}: {v}")

if r["status"] == "ok":
    print("\n=== Metadados predicao_t1 ===")
    meta = conn.execute("SELECT * FROM predicao_t1_metadados").df()
    for col in meta.columns:
        print(f"  {col}: {meta.iloc[0][col]}")

    print("\n=== Top 8 H3 por deficit_calibrado_t1 ===")
    top = conn.execute("""
        SELECT h3_index,
               ROUND(deficit_calibrado_t1, 1) AS def_t1,
               ROUND(deficit_ic_lower_t1, 1)  AS ic_low,
               ROUND(deficit_ic_upper_t1, 1)  AS ic_up,
               ROUND(delta_aplicado, 2)        AS delta
        FROM deficit_calibrado_h3_t1
        ORDER BY deficit_calibrado_t1 DESC NULLS LAST
        LIMIT 8
    """).df()
    print(top.to_string(index=False))

    print("\n=== Comparação t0 vs t1 por H3 (amostra 8 maiores t0) ===")
    comp = conn.execute("""
        SELECT t0.h3_index,
               ROUND(t0.deficit_calibrado, 1) AS def_t0,
               ROUND(t1.deficit_calibrado_t1, 1) AS def_t1,
               ROUND(t1.deficit_calibrado_t1 - t0.deficit_calibrado, 1) AS variacao,
               ROUND(t1.delta_aplicado, 2) AS delta_proxy_abs
        FROM deficit_calibrado_h3_t0 t0
        JOIN deficit_calibrado_h3_t1 t1 ON t0.h3_index = t1.h3_index
        ORDER BY t0.deficit_calibrado DESC NULLS LAST
        LIMIT 8
    """).df()
    print(comp.to_string(index=False))

    print("\n=== Estatísticas globais ===")
    stats = conn.execute("""
        SELECT
            COUNT(*) AS n_hex,
            COUNT(deficit_calibrado_t1) AS n_com_valor,
            ROUND(SUM(deficit_calibrado_t1), 1) AS total_t1,
            ROUND(AVG(deficit_calibrado_t1), 2) AS media_h3,
            ROUND(MIN(deficit_calibrado_t1), 2) AS minimo,
            ROUND(MAX(deficit_calibrado_t1), 1) AS maximo
        FROM deficit_calibrado_h3_t1
    """).df()
    print(stats.to_string(index=False))

conn.close()
print("\nTeste Etapa 8 concluído.")
