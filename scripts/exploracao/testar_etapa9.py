"""Testa a Etapa 9 (validação — Arapiraca, t0=2022, t1=2024)."""
import sys
import logging
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")
logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
sys.path.insert(0, str(Path(__file__).parents[2]))

from modulo_coleta.utils.db_utils import abrir_conexao
from modulo_estimacao.etapa9_validacao import validar_estimativas

DB = Path("data/processed/al_arapiraca/al_arapiraca.duckdb")
conn = abrir_conexao(DB)

r = validar_estimativas("2700300", db_conn=conn, comparar_fjp=True)

print("\n=== Resultado Etapa 9 ===")
print(f"  status  : {r['status']}")
print(f"  alertas : {r['alertas']}")

print("\n=== Módulo A — CV 5-fold ===")
ma = r.get("modulo_a", {})
for k, v in ma.items():
    print(f"  {k}: {v}")

print("\n=== Módulo B — Moran's I ===")
mb = r.get("modulo_b", {})
print(f"  moran_i = {mb.get('moran_i')} | moran_p = {mb.get('moran_p')}")

print("\n=== Módulo C — Consistência de escala ===")
mc = r.get("modulo_c", {})
for periodo, vals in mc.items():
    print(f"  {periodo}: {vals}")

print("\n=== Módulo D — Comparação FJP ===")
md = r.get("modulo_d", {})
for k, v in md.items():
    print(f"  {k}: {v}")

print("\n=== Módulo E — Sanity checks ===")
me = r.get("modulo_e", {})
for k, v in me.items():
    print(f"  {k}: {v}")

print("\n=== Resumo salvo (validacao_resumo) ===")
resumo = conn.execute("SELECT * FROM validacao_resumo").df()
for col in resumo.columns:
    print(f"  {col}: {resumo.iloc[0][col]}")

conn.close()
print("\nTeste Etapa 9 concluído.")
