"""
etapas_t1 — sub-pipeline do ano-corrente (sem Censo, apenas covariáveis anuais).

Etapas:
    6. covariaveis_t1     — recalcula apenas as covariáveis que mudam entre t0 e t1
                            (luminosidade VIIRS + MapBiomas)
    7. modelo_temporal    — regressão Δdéficit ~ Δluminosidade + Δuso_solo no setor
    8. predicao_h3_t1     — aplica delta em H3; calibra com PNADc t1 se disponível

Pré-requisito: sub-pipeline t0 completo (etapas 1-5).
"""
