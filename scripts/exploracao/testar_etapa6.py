"""Testa a Etapa 6 (covariáveis t1 — Arapiraca, t0=2022, t1=2024)."""
import sys
import logging
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")
logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
sys.path.insert(0, str(Path(__file__).parents[2]))

from modulo_coleta.utils.db_utils import abrir_conexao
from modulo_estimacao.etapas_t1.etapa6_covariaveis_t1 import extrair_covariaveis_setor_t1

DB = Path("data/processed/al_arapiraca/al_arapiraca.duckdb")
conn = abrir_conexao(DB)

r = extrair_covariaveis_setor_t1("2700300", ano_t0=2022, ano_t1=2024, db_conn=conn)
print("\n=== Resultado Etapa 6 ===")
for k, v in r.items():
    print(f"  {k}: {v}")

if r["status"] == "ok":
    print("\n=== covariaveis_setor_t1 — primeiras 5 linhas ===")
    cov = conn.execute("""
        SELECT cod_setor,
               ROUND(luminosidade_mean_t1, 4) AS lum_mean_t1,
               ROUND(luminosidade_std_t1, 4)  AS lum_std_t1,
               ROUND(prop_urbano_t1, 4)        AS prop_urb_t1,
               ROUND(prop_mosaico_uso_t1, 4)   AS prop_mos_t1,
               ROUND(prop_vegetacao_t1, 4)     AS prop_veg_t1
        FROM covariaveis_setor_t1
        ORDER BY cod_setor
        LIMIT 5
    """).df()
    print(cov.to_string(index=False))

    print("\n=== delta_covariaveis_setor — top 8 por delta_prop_urbano ===")
    delta = conn.execute("""
        SELECT cod_setor,
               ROUND(delta_lum_mean, 4)      AS d_lum_mean,
               ROUND(delta_prop_urbano, 4)   AS d_urb,
               ROUND(delta_prop_mosaico, 4)  AS d_mos,
               ROUND(delta_prop_vegetacao, 4) AS d_veg,
               flag_expansao
        FROM delta_covariaveis_setor
        ORDER BY delta_prop_urbano DESC NULLS LAST
        LIMIT 8
    """).df()
    print(delta.to_string(index=False))

    print("\n=== Estatísticas das variações ===")
    stats = conn.execute("""
        SELECT
            COUNT(*) AS n_setores,
            SUM(flag_expansao::INT) AS n_expansao,
            ROUND(AVG(delta_lum_mean), 4)     AS delta_lum_medio,
            ROUND(AVG(delta_prop_urbano), 4)  AS delta_urb_medio,
            ROUND(MIN(delta_prop_urbano), 4)  AS delta_urb_min,
            ROUND(MAX(delta_prop_urbano), 4)  AS delta_urb_max
        FROM delta_covariaveis_setor
    """).df()
    print(stats.to_string(index=False))

    print("\n=== Comparação t0 vs t1 — luminosidade média municipal ===")
    comp = conn.execute("""
        SELECT
            ROUND(AVG(t0.luminosidade_setor_mean), 4) AS lum_mean_t0,
            ROUND(AVG(t1.luminosidade_mean_t1), 4)    AS lum_mean_t1,
            ROUND(AVG(t1.luminosidade_mean_t1) - AVG(t0.luminosidade_setor_mean), 4) AS delta
        FROM covariaveis_setor_t0 t0
        JOIN covariaveis_setor_t1 t1 ON t0.cod_setor = t1.cod_setor
    """).df()
    print(comp.to_string(index=False))

conn.close()
print("\nTeste Etapa 6 concluído.")
