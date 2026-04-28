"""Testa a Etapa 5 (calibração IPF t0)."""
import sys
import logging
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")
logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
sys.path.insert(0, str(Path(__file__).parents[2]))

from modulo_coleta.utils.db_utils import abrir_conexao
from modulo_estimacao.etapas_t0.etapa5_calibracao_t0 import calibrar_h3_t0

DB = Path("data/processed/al_arapiraca/al_arapiraca.duckdb")
conn = abrir_conexao(DB)

r = calibrar_h3_t0("2700300", conn, tolerancia=1e-4, max_iter=50, usar_ancora_pnadc=True)
print("\n=== Resultado Etapa 5 ===")
for k, v in r.items():
    print(f"  {k}: {v}")

if r["status"] == "ok":
    print("\n=== Metadados da calibração ===")
    meta = conn.execute("SELECT chave, valor FROM calibracao_metadados_t0 ORDER BY chave").df()
    print(meta.to_string(index=False))

    print("\n=== Top 8 hexágonos por deficit_calibrado ===")
    top = conn.execute("""
        SELECT h3_index,
               ROUND(deficit_predito, 1) AS predito,
               ROUND(deficit_calibrado, 1) AS calibrado,
               ROUND(fator_calibracao_setor, 3) AS f_setor,
               ROUND(fator_calibracao_dominio, 3) AS f_dominio,
               cod_setor_dominante
        FROM deficit_calibrado_h3_t0
        ORDER BY deficit_calibrado DESC NULLS LAST
        LIMIT 8
    """).df()
    print(top.to_string(index=False))

    print("\n=== Estatísticas globais ===")
    stats = conn.execute("""
        SELECT
            COUNT(*) AS n_hex,
            COUNT(deficit_calibrado) AS n_calibrados,
            ROUND(SUM(deficit_predito), 1) AS total_predito,
            ROUND(SUM(deficit_calibrado), 1) AS total_calibrado,
            ROUND(AVG(fator_calibracao_setor), 4) AS fator_setor_medio,
            ROUND(AVG(fator_calibracao_dominio), 4) AS fator_dominio_medio
        FROM deficit_calibrado_h3_t0
    """).df()
    print(stats.to_string(index=False))

    print("\n=== Verificação: soma H3 por setor vs. alvo setor ===")
    check = conn.execute("""
        SELECT
            c.cod_setor_dominante AS cod_setor,
            ROUND(SUM(c.deficit_calibrado), 2) AS soma_h3_calibrado,
            ROUND(p.proxy_carencias_setor * p.n_dom_total, 2) AS alvo_setor,
            ROUND(SUM(c.deficit_calibrado) - p.proxy_carencias_setor * p.n_dom_total, 2) AS diferenca
        FROM deficit_calibrado_h3_t0 c
        JOIN proxy_setor p ON c.cod_setor_dominante = p.cod_setor
        GROUP BY c.cod_setor_dominante, p.proxy_carencias_setor, p.n_dom_total
        ORDER BY ABS(SUM(c.deficit_calibrado) - p.proxy_carencias_setor * p.n_dom_total) DESC
        LIMIT 10
    """).df()
    print(check.to_string(index=False))

conn.close()
print("\nTeste Etapa 5 concluído.")
