"""Testa a Etapa 3 (modelo espacial) com RF e LM."""
import sys
import logging
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")
logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
sys.path.insert(0, str(Path(__file__).parents[2]))

from modulo_coleta.utils.db_utils import abrir_conexao
from modulo_estimacao.etapas_t0.etapa3_modelo_espacial import ajustar_modelo_t0

DB = Path("data/processed/al_arapiraca/al_arapiraca.duckdb")
conn = abrir_conexao(DB)

for mod in ["rf", "lm"]:
    print(f"\n=== Modelo: {mod} ===")
    r = ajustar_modelo_t0("2700300", 2022, conn, modelo=mod, output_dir=Path("data/processed/al_arapiraca"))
    for k, v in r.items():
        if k not in ("pkl_path",):
            print(f"  {k}: {v}")

# Diagnósticos salvos
print("\n=== modelo_t0_diagnostico (DuckDB) ===")
diag = conn.execute("SELECT modelo, r2_treino, rmse_treino, cv_rmse_y, cv_rmse_5fold, moran_i_residuos, moran_p, n_setores FROM modelo_t0_diagnostico").df()
print(diag.to_string(index=False))

conn.close()
print("\nTeste Etapa 3 concluido.")
