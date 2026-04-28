"""Testa a Etapa 7 (modelo temporal — Arapiraca, t0=2022, t1=2024)."""
import sys
import logging
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")
logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
sys.path.insert(0, str(Path(__file__).parents[2]))

from modulo_coleta.utils.db_utils import abrir_conexao
from modulo_estimacao.etapas_t1.etapa7_modelo_temporal import ajustar_modelo_temporal

DB       = Path("data/processed/al_arapiraca/al_arapiraca.duckdb")
OUT_DIR  = Path("data/processed/al_arapiraca")
conn = abrir_conexao(DB)

r = ajustar_modelo_temporal(
    "2700300", ano_t0=2022, ano_t1=2024,
    db_conn=conn, modelo="lm", output_dir=OUT_DIR,
)
print("\n=== Resultado Etapa 7 ===")
for k, v in r.items():
    print(f"  {k}: {v}")

if r["status"] == "ok":
    print("\n=== Diagnóstico temporal ===")
    diag = conn.execute("""
        SELECT modelo, ROUND(r2,4) AS r2, ROUND(rmse,5) AS rmse, n_setores,
               ROUND(coef_delta_lum,5)     AS coef_lum,
               ROUND(coef_delta_urbano,5)  AS coef_urb,
               ROUND(coef_flag_expansao,5) AS coef_exp,
               ROUND(coef_proxy_t0,4)      AS coef_prx_t0
        FROM modelo_temporal_diagnostico
    """).df()
    print(diag.to_string(index=False))

    print("\n=== Top 8 setores por delta_proxy_predito ===")
    top = conn.execute("""
        SELECT cod_setor,
               ROUND(delta_proxy_predito, 5)  AS d_proxy,
               ROUND(delta_proxy_ic_lower, 5) AS ic_low,
               ROUND(delta_proxy_ic_upper, 5) AS ic_up
        FROM delta_proxy_setor_predito
        ORDER BY delta_proxy_predito DESC NULLS LAST
        LIMIT 8
    """).df()
    print(top.to_string(index=False))

    print("\n=== Estatísticas do delta_proxy ===")
    stats = conn.execute("""
        SELECT
            COUNT(*) AS n_setores,
            SUM(CASE WHEN delta_proxy_predito > 0 THEN 1 ELSE 0 END) AS n_positivo,
            ROUND(AVG(delta_proxy_predito), 5)  AS media,
            ROUND(MIN(delta_proxy_predito), 5)  AS minimo,
            ROUND(MAX(delta_proxy_predito), 5)  AS maximo,
            ROUND(STDDEV(delta_proxy_predito), 5) AS desvio_pad
        FROM delta_proxy_setor_predito
    """).df()
    print(stats.to_string(index=False))

    print("\n=== Sanidade: flag_expansao vs delta_proxy ===")
    san = conn.execute("""
        SELECT d.flag_expansao,
               COUNT(*) AS n,
               ROUND(AVG(p.delta_proxy_predito), 5) AS delta_proxy_medio
        FROM delta_covariaveis_setor d
        JOIN delta_proxy_setor_predito p ON d.cod_setor = p.cod_setor
        GROUP BY d.flag_expansao
        ORDER BY d.flag_expansao
    """).df()
    print(san.to_string(index=False))

conn.close()
print("\nTeste Etapa 7 concluído.")
