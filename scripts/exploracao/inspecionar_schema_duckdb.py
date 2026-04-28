"""Inspeciona schema das tabelas do DuckDB de Arapiraca relevantes para Etapa 1."""
import json
import sys
from pathlib import Path
import duckdb

# Forca UTF-8 na saida
sys.stdout.reconfigure(encoding="utf-8")

DB = Path("data/processed/al_arapiraca/al_arapiraca.duckdb")
conn = duckdb.connect(str(DB))
conn.execute("INSTALL spatial; LOAD spatial")

# Lista tabelas existentes
todas = conn.execute("SHOW TABLES").fetchdf()
print("Tabelas no banco:", list(todas["name"]))

tabelas = [
    "setores_censitarios",
    "censo_domicilio01",
    "censo_domicilio02",
    "censo_responsavel01",
]

resultado = {}
for t in tabelas:
    try:
        schema = conn.execute(f"DESCRIBE {t}").fetchdf()
        resultado[t] = schema.to_dict(orient="records")
        print(f"\n=== {t} ({len(schema)} colunas) ===")
        print(schema[["column_name", "column_type"]].to_string(index=False))

        # Primeiras 2 linhas para ver valores reais
        sample = conn.execute(f"SELECT * FROM {t} LIMIT 2").fetchdf()
        print("  -> colunas (primeiras 10):", list(sample.columns[:10]))
        if len(sample) > 0:
            row = {k: v for k, v in list(sample.iloc[0].items())[:10]}
            print(f"  -> 1a linha: {row}")
    except Exception as e:
        resultado[t] = {"erro": str(e)}
        print(f"\n=== {t} -> ERRO: {e} ===")

out = Path("data/processed/schemas/schema_etapa1.json")
out.parent.mkdir(parents=True, exist_ok=True)
out.write_text(json.dumps(resultado, indent=2, ensure_ascii=False), encoding="utf-8")
print(f"\nSalvo em {out}")
conn.close()
