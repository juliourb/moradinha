"""
modulo_estimacao — Pipeline de estimação espacial e temporal do déficit habitacional.

Pipeline em dois sub-períodos paralelos:

    t0 (ano-base): Censo 2022 disponível → proxy setor → modelo espacial → H3 calibrado
    t1 (ano-corrente): sem Censo → variação de covariáveis → modelo temporal → H3 atualizado

Uso mínimo:
    from modulo_estimacao.orquestrador import estimar_municipio
    estimar_municipio("2700300", ano_t0=2022, ano_t1=2024, db_path=Path("data/processed/..."))
"""
