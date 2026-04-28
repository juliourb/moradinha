"""
Coleta covariáveis anuais para t1=2024 (Arapiraca).

Popula no DuckDB:
    luminosidade_2024         — zonal stats VIIRS por setor
    luminosidade_2024_grade200 — zonal stats VIIRS por célula grade 200m
    mapbiomas_2024            — proporções de uso do solo por setor
"""
import sys
import logging
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")
logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
sys.path.insert(0, str(Path(__file__).parents[2]))

from modulo_coleta.utils.db_utils import abrir_conexao
from modulo_coleta.utils.raster_utils import ler_tabela_espacial
from modulo_coleta.grupos.grupo4_luminosidade import coletar_grupo4
from modulo_coleta.grupos.grupo6_uso_solo_precariedade import coletar_grupo6

BASE_DIR   = Path("data")
MUN_DIR    = BASE_DIR / "raw" / "al_arapiraca"
DB_PATH    = BASE_DIR / "processed" / "al_arapiraca" / "al_arapiraca.duckdb"
TILES_DIR  = BASE_DIR / "raw" / "tiles_globais"

# Tile VIIRS 2024 — explícito para evitar seleção do tile 2022
TILE_VIIRS_2024 = TILES_DIR / "VNL_npp_2024_global_vcmslcfg_v2_c202502261200.average_masked.dat.tif"

conn = abrir_conexao(DB_PATH)
limite_municipal = ler_tabela_espacial(conn, "limite_municipal")

# --- Grupo 4 — Luminosidade 2024 ---
print("\n=== Grupo 4 — Luminosidade 2024 ===")
r4 = coletar_grupo4(
    codigo_ibge="2700300",
    limite_municipal=limite_municipal,
    output_dir=MUN_DIR / "luminosidade",
    db_conn=conn,
    modo="tile_local",
    tile_path=TILE_VIIRS_2024,
    ano=2024,
    forcar=False,
)
print(f"  status : {r4['status']}")
print(f"  camadas: {r4.get('camadas', [])}")
print(f"  msg    : {r4.get('mensagem', '')}")

# --- Grupo 6 — MapBiomas 2024 ---
print("\n=== Grupo 6 — MapBiomas 2024 ===")
r6 = coletar_grupo6(
    codigo_ibge="2700300",
    limite_municipal=limite_municipal,
    output_dir=MUN_DIR / "uso_solo",
    db_conn=conn,
    anos_mapbiomas=[2024],
    tile_dir=TILES_DIR,
    fcu_cache_dir=BASE_DIR / "raw" / "cache_fcu",
    forcar=False,
)
print(f"  status : {r6['status']}")
print(f"  camadas: {r6.get('camadas', [])}")
print(f"  msg    : {r6.get('mensagem', '')}")

# --- Verificar tabelas geradas ---
print("\n=== Tabelas disponíveis (2024) ===")
tabelas_2024 = [r[0] for r in conn.execute("SHOW TABLES").fetchall() if "2024" in r[0]]
for t in sorted(tabelas_2024):
    n = conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
    print(f"  {t}: {n} linhas")

conn.close()
print("\nColeta t1 concluída.")
