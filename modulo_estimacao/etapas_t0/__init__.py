"""
etapas_t0 — sub-pipeline do ano-base (Censo 2022 disponível).

Etapas:
    1. proxy_setor        — proxy do déficit FJP via Censo 2022 por setor
    2. covariaveis_t0     — matriz de covariáveis territoriais no setor (t0)
    3. modelo_espacial    — ajuste do modelo proxy ~ covariáveis
    4. predicao_h3_t0     — projeção das predições para grade H3
    5. calibracao_t0      — IPF ancorado em setor (Censo) e domínio (PNADc t0)
"""
