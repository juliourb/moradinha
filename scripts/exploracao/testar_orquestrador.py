"""
Testa o orquestrador do modulo_estimacao — Arapiraca, t0=2022, t1=2024.

Roda apenas etapas 8 e 9 (as mais recentes) assumindo que 1-7 já estão no DB.
Para rodar o pipeline completo do zero, use etapas=list(range(1, 10)).
"""
import sys
import logging
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")
logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
sys.path.insert(0, str(Path(__file__).parents[2]))

from modulo_estimacao.orquestrador import estimar_municipio

DB     = Path("data/processed/al_arapiraca/al_arapiraca.duckdb")
OUT    = Path("data/processed/al_arapiraca")

# Teste 1: rodar apenas etapas 8-9 (com forcar=True para reprocessar)
print("=== Teste: etapas 8-9 (forçado) ===")
r = estimar_municipio(
    codigo_ibge="2700300",
    ano_t0=2022,
    ano_t1=2024,
    db_path=DB,
    resolucao_h3=8,
    etapas=[8, 9],
    modelo_t0="rf",
    modelo_temporal="lm",
    output_dir=OUT,
    forcar=True,
)
print(f"\n  status          : {r['status']}")
print(f"  mensagem        : {r['mensagem']}")
print(f"  etapas_exec     : {r['etapas_executadas']}")
print(f"  etapas_ok       : {r['etapas_ok']}")
print(f"  etapas_erro     : {r['etapas_erro']}")

# Teste 2: rodar etapas 1-9 com forcar=False (todas devem ser puladas)
print("\n=== Teste: etapas 1-9 com forcar=False (deve pular tudo) ===")
r2 = estimar_municipio(
    codigo_ibge="2700300",
    ano_t0=2022,
    ano_t1=2024,
    db_path=DB,
    resolucao_h3=8,
    etapas=list(range(1, 10)),
    output_dir=OUT,
    forcar=False,
)
print(f"\n  status          : {r2['status']}")
print(f"  etapas_puladas  : {r2['etapas_puladas']}")
print(f"  etapas_exec     : {r2['etapas_executadas']}")

# Teste 3: DB inexistente → erro gracioso
print("\n=== Teste: DB inexistente (deve retornar erro) ===")
r3 = estimar_municipio(
    codigo_ibge="9999999",
    ano_t0=2022,
    ano_t1=2024,
    db_path=Path("data/processed/inexistente/db.duckdb"),
)
print(f"  status: {r3['status']} | msg: {r3['mensagem']}")

print("\nTeste orquestrador concluído.")
