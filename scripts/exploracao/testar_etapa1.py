"""Testa a Etapa 1 do modulo_estimacao com o DuckDB de Arapiraca."""
import sys
import logging
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")
logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

# Adiciona raiz do projeto ao path
sys.path.insert(0, str(Path(__file__).parents[2]))

from modulo_coleta.utils.db_utils import abrir_conexao
from modulo_estimacao.etapas_t0.etapa1_proxy_setor import calcular_proxy_setor

DB = Path("data/processed/al_arapiraca/al_arapiraca.duckdb")
conn = abrir_conexao(DB)

resultado = calcular_proxy_setor("2700300", conn, salvar=True)
print("\n=== Resultado da Etapa 1 ===")
for k, v in resultado.items():
    print(f"  {k}: {v}")

# Verificar tabela salva
if resultado["status"] == "ok":
    df = conn.execute("SELECT * FROM proxy_setor LIMIT 5").df()
    print(f"\n=== proxy_setor (primeiras 5 linhas) ===")
    print(df[["cod_setor", "n_dom_total", "proxy_carencias_setor", "renda_responsavel_media"]].to_string(index=False))

    # Estatisticas descritivas das proporcoes
    print("\n=== Estatisticas das proporcoes ===")
    prop_cols = [c for c in df.columns if c.startswith("prop_")]
    df_all = conn.execute("SELECT * FROM proxy_setor").df()
    print(df_all[["proxy_carencias_setor"] + prop_cols].describe().round(4).to_string())

conn.close()
print("\nTeste concluido.")
