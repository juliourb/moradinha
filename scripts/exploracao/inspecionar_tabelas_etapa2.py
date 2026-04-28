"""Inspeciona schemas e amostras das tabelas-fonte da Etapa 2."""
import json
import sys
from pathlib import Path
sys.stdout.reconfigure(encoding="utf-8")
sys.path.insert(0, str(Path(__file__).parents[2]))

from modulo_coleta.utils.db_utils import abrir_conexao

conn = abrir_conexao(Path("data/processed/al_arapiraca/al_arapiraca.duckdb"))

tabelas = [
    "luminosidade_2022",
    "mapbiomas_2022",
    "fcu_setor",
    "enderecos_cnefe_residencial",
    "enderecos_cnefe_naoresidencial",
    "eixos_osm",
    "grade_estatistica",
]

resultado = {}
for t in tabelas:
    try:
        schema = conn.execute(f"DESCRIBE {t}").fetchdf()
        n_rows = conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
        sample = conn.execute(f"SELECT * FROM {t} LIMIT 2").fetchdf()
        resultado[t] = {
            "n_rows": n_rows,
            "columns": schema[["column_name", "column_type"]].to_dict(orient="records"),
        }
        print(f"\n=== {t} ({n_rows} linhas, {len(schema)} colunas) ===")
        print(schema[["column_name", "column_type"]].to_string(index=False))
        if len(sample) > 0:
            print(f"  amostra (primeiras cols): {dict(list(sample.iloc[0].items())[:8])}")
    except Exception as e:
        resultado[t] = {"erro": str(e)}
        print(f"\n=== {t} -> ERRO: {e} ===")

out = Path("data/processed/schemas/schema_etapa2.json")
out.parent.mkdir(parents=True, exist_ok=True)
out.write_text(json.dumps(resultado, indent=2, ensure_ascii=False), encoding="utf-8")
print(f"\nSalvo em {out}")
conn.close()
