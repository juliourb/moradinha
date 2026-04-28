"""Testa a Etapa 2 do modulo_estimacao com o DuckDB de Arapiraca."""
import sys
import logging
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")
logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

sys.path.insert(0, str(Path(__file__).parents[2]))

from modulo_coleta.utils.db_utils import abrir_conexao
from modulo_estimacao.etapas_t0.etapa2_covariaveis_t0 import (
    extrair_covariaveis_setor_t0,
    extrair_covariaveis_h3_t0,
)

DB = Path("data/processed/al_arapiraca/al_arapiraca.duckdb")
conn = abrir_conexao(DB)

# --- Etapa 2a: covariaveis_setor_t0 ---
print("\n=== Etapa 2a: covariaveis_setor_t0 ===")
r = extrair_covariaveis_setor_t0("2700300", 2022, conn, salvar=True)
for k, v in r.items():
    print(f"  {k}: {v}")

if r["status"] == "ok":
    df = conn.execute("SELECT * EXCLUDE geometry FROM covariaveis_setor_t0 LIMIT 5").df()
    print("\n  Primeiras 5 linhas:")
    print(df.to_string(index=False))
    print("\n  Estatisticas:")
    full = conn.execute("SELECT * EXCLUDE geometry FROM covariaveis_setor_t0").df()
    print(full.describe().round(3).to_string())

# --- Etapa 2b: covariaveis_h3_t0 ---
print("\n=== Etapa 2b: covariaveis_h3_t0 ===")
r2 = extrair_covariaveis_h3_t0("2700300", 2022, 8, conn, salvar=True)
for k, v in r2.items():
    print(f"  {k}: {v}")

if r2["status"] == "ok":
    df_h3 = conn.execute("SELECT * FROM covariaveis_h3_t0 LIMIT 5").df()
    print("\n  Primeiras 5 linhas:")
    print(df_h3.to_string(index=False))

conn.close()
print("\nTeste Etapa 2 concluido.")
