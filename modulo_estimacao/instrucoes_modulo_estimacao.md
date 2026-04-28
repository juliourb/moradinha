# Instruções — Módulo de Estimação

Pipeline de estimação espacial e temporal do déficit habitacional por hexágono H3.

---

## 1. Visão geral

O `modulo_estimacao` transforma as covariáveis territoriais coletadas pelo `modulo_coleta`
em estimativas intraurbanas de déficit habitacional por hexágono H3, em dois períodos:

| Período | Fonte principal | Etapas |
|---|---|---|
| **t0** (ano-base) | Censo 2022 + todas as fontes | 1 → 5 |
| **t1** (ano-corrente) | VIIRS + MapBiomas anuais | 6 → 8 |
| **Validação** | Transversal (t0 e/ou t1) | 9 |

Pré-requisito: DuckDB do município já populado pelo `modulo_coleta` (Grupos 1–6).

---

## 2. Pipeline em dois períodos (t0 + t1)

### Sub-pipeline t0 — ano-base (Censo disponível)

```
[Censo 2022]              [Covariáveis t0]
 Etapa 1                   Etapa 2
 proxy_setor  ──────────►  covariaveis_setor_t0 ──► covariaveis_h3_t0
      │                                                      │
      │                    Etapa 3                           │
      └───────────────►   modelo_espacial ◄──────────────────┘
                               │
                           Etapa 4
                        predicao_h3_t0
                               │
                           Etapa 5
                        calibracao_t0  ◄── [PNADc t0]
                               │
                    deficit_calibrado_h3_t0
```

### Sub-pipeline t1 — ano-corrente (sem Censo)

```
[VIIRS t1 + MapBiomas t1]
  Etapa 6
  covariaveis_setor_t1
  delta_covariaveis_setor
        │
    Etapa 7
  modelo_temporal  ◄── [proxy_setor Etapa 1]
        │
    Etapa 8
  predicao_h3_t1  ◄── [deficit_calibrado_h3_t0]  ◄── [PNADc t1, se disponível]
        │
  deficit_calibrado_h3_t1
```

### Validação transversal

```
[deficit_calibrado_h3_t0]  [deficit_calibrado_h3_t1]
            └──────────────────────┘
                    Etapa 9
                  validacao
                      │
        [validacao_resumo no DuckDB]
```

---

## 3. Estado atual de implementação

| Etapa | Período | Status | Observações |
|---|---|---|---|
| 1. proxy_setor | t0 | ✅ Implementada | proxy_carencias_setor; 6 proporções + pesos iguais; renda covariável; geom de setores_censitarios |
| 2. covariaveis_t0 | t0 | ✅ Implementada | 426 setores / 431 hex H3 res=8; CNEFE via DuckDB spatial; dist/buffer via GeoPandas EPSG:5880 |
| 3. modelo_espacial | t0 | ✅ Implementada | RF R²=0.76, Moran I=0.033 (p=0.24, ok); LM R²=0.30; GWR via mgwr opcional |
| 4. predicao_h3_t0 | t0 | ✅ Implementada | 431 hex res=8; IC via std entre árvores RF; deficit_estimado_total=14.186 (pré-calib) |
| 5. calibracao_t0 | t0 | ✅ Implementada | Calibração direta 2 passos: distribuição Censo → âncora PNADc; fator_dominio=0.682 (CV PNADc=73%, cv_alto); deficit_calibrado=3749 |
| 6. covariaveis_t1 | t1 | ✅ Implementada | 426 setores; 223/426 lum_t1 preenchidos; 354/426 MapBiomas_t1; 27 setores flag_expansao; delta_lum_dominio=7.79 |
| 7. modelo_temporal | t1 | ✅ Implementada | Aplica RF t0 em features t1 → proxy_t1_predito; diagnóstico LM R²=0.71 (linearização RF); 222/426 setores no diagnóstico; delta_proxy_medio=−0.018 |
| 8. predicao_h3_t1 | t1 | ✅ Implementada | 431 H3; 429 com delta; t0=3749 → t1=3587.8 (−4.3%); PNADc t1 indisponível (ano 2022≠2024) |
| 9. validacao | transversal | ✅ Implementada | Módulos A–E; R²_cv=0.38; Moran I=0.034 (p=0.23, ok); consistência t0: soma_h3=PNADc=3749; FJP indisponível para Arapiraca |
| Orquestrador | todos | ✅ Implementado | Encadeia E1–E9; pula etapas já salvas (forcar=False); tratamento de erros por etapa |

---

## 4. Covariáveis canônicas do modelo (Etapa 2)

| # | Covariável | Fonte | Filtro |
|---|---|---|---|
| 1 | renda_resp_media | Grupo 2 — responsavel01 | — |
| 2 | luminosidade_setor_mean | Grupo 4 — luminosidade_{ano_t0} | — |
| 3 | luminosidade_setor_std | Grupo 4 — luminosidade_{ano_t0} | — |
| 4 | cnefe_residencial_densidade | Grupo 3 — enderecos_cnefe_residencial | qualidade_geo IN (alta, media) |
| 5 | cnefe_naoresid_densidade | Grupo 3 — enderecos_cnefe_naoresidencial | qualidade_geo IN (alta, media) |
| 6 | prop_urbano | Grupo 6 — mapbiomas_{ano_t0} | — |
| 7 | prop_mosaico_uso | Grupo 6 — mapbiomas_{ano_t0} | — |
| 8 | prop_vegetacao | Grupo 6 — mapbiomas_{ano_t0} | — |
| 9 | fcu_intersecta | Grupo 6 — fcu_setor | — |
| 10 | fcu_area_pct | Grupo 6 — fcu_setor | — |
| 11 | dist_centro_m | Calculada via eixos OSM (Grupo 3) | — |
| 12 | cnefe_densidade_buffer_500m | Calculada via Grupo 3 | qualidade_geo IN (alta, media) |

**Excluídas do modelo** (mas mantidas como insumos estruturais):
- Eixos OSM, faces de logradouro: colineares com CNEFE como features; usados para `dist_centro_m` e Moran's I
- Grade estatística IBGE 200m: usada na Etapa 4 para `n_domicilios_grade` em H3
- prop_alvenaria, prop_saneamento: vazamento conceitual (componentes do proxy-alvo)

---

## 5. Decisões metodológicas estruturais

| Data | Decisão | Alternativa rejeitada | Motivo |
|---|---|---|---|
| 2026-04-26 | Luminosidade NÃO mascarada pelo MapBiomas | Mascarar pixels vegetados antes do zonal stats | Preserva independência das fontes; sustenta Etapa 7 com vetor limpo; RF robusto à colinearidade fraca |
| 2026-04-26 | Pipeline t0 + t1 paralelos | Pipeline linear único | Reflete inspiração BID 2019 adaptada à escala intraurbana; permite atualização anual sem reajustar modelo espacial |
| 2026-04-26 | Anos como parâmetros obrigatórios | Download dinâmico de tiles | Tiles VIIRS e MapBiomas (>10 GB) mantidos em `data/raw/tiles_globais/` pelo pesquisador |
| 2026-04-26 | proxy_total_t0 como covariável da Etapa 7 | Não incluir proxy_t0 no modelo temporal | Controla regressão à média: unidades extremas em t0 tendem a parecer mais moderadas em t1 mesmo sem mudança real |
| 2026-04-26 | Ônus de aluguel excluído do proxy-alvo (variável `proxy_total_sem_onus`) | Incluir ônus no alvo | Vazamento: ônus é melhor estimado via PNADc (renda declarada vs. declaração Censo); incluso na calibração macro via Etapa 5 |

---

## 6. Dependências externas

```
scikit-learn    — Random Forest (Etapas 3, 7)
statsmodels     — OLS robusto (Etapas 3, 7)
mgwr            — GWR/MGWR (Etapa 3 — opcional)
h3-py           — Grade hexagonal H3 (Etapas 2, 4, 8)
esda            — Moran's I (Etapas 3, 9)
libpysal        — Pesos espaciais Queen (Etapas 3, 9)
numpy, pandas   — Álgebra linear, manipulação tabular
geopandas       — GeoDataFrame para geometria H3
duckdb          — Leitura/escrita do banco do município
```

---

## 7. Artefato canônico de referência metodológica

**Localização:** `data/raw/referencias/mapeamento_fjp_censo2022.json`

Este arquivo é a especificação curada que mapeia as regras FJP para variáveis do Censo 2022.
Foi validado contra o dicionário XLSX local em 2026-04-26, gerando:

- `data/processed/dicionarios/dicionario_censo2022_parsed.json` — dicionário parseado (reutilizável)
- `data/processed/dicionarios/mapeamento_fjp_censo2022_validado.json` — versão preenchida e com decisões
- `data/processed/dicionarios/relatorio_validacao_mapeamento.md` — relatório de cobertura

A Etapa 1 deve consumir o arquivo **validado**, não o curado.

**Cobertura final (2026-04-26):**

| Componente FJP | Variável Censo | Status |
|---|---|---|
| A2 — Improvisados | V00002 | ✅ Disponível no Universo |
| B1 — Cômodos/Cortiço | V00050 | ✅ Disponível no Universo |
| A1 — Rústicos (4 categorias) | — | ⏳ Só na Amostra (não publicada) |
| B2 — Adensamento dormitório (4 células) | — | ⏳ Só na Amostra (não publicada) |
| C — Ônus aluguel | — | ⏳ Só na Amostra (não publicada) |
| Denominador DPPO | V00001 | ✅ |
| Renda responsável (covariável) | V06004 | ✅ |
| Sem banheiro (índice) | V00238 | ✅ |
| Água rede geral (índice) | V00111 | ✅ |
| Esgoto rede geral (índice) | V00309 | ✅ |
| Lixo coletado (índice) | V00397 | ✅ |

---

## 8. Índice composto de carências habitacionais (proxy_carencias_setor)

Substituição do proxy de componentes FJP individuais enquanto os agregados da Amostra não são publicados.

```
proxy_carencias_setor = w1·prop_improvisados        (V00002 / V00001)
                      + w2·prop_comodos_cortico      (V00050 / V00001)
                      + w3·prop_sem_banheiro         (V00238 / V00001)
                      + w4·(1 - prop_agua_rede)      (1 - V00111 / V00001)
                      + w5·(1 - prop_esgoto_rede)    (1 - V00309 / V00001)
                      + w6·(1 - prop_lixo_coletado)  (1 - V00397 / V00001)
```

**Pesos w1..w6:** determinados na Etapa 3 (não arbitrados a priori). Estratégias disponíveis:

- Pesos iguais → índice somativo simples
- PCA → primeiro componente principal das 6 variáveis de carência
- Regressão → pesos que maximizam correlação com o total FJP do domínio PNADc

**Papel no pipeline:** variável-alvo da Etapa 3 (Random Forest). Serve como ranqueador relativo de intensidade de carência por setor. Os totais absolutos são ancorados ao PNADc no domínio via IPF na Etapa 5.

**Nota sobre prop_sem_banheiro:** entra com peso alto — ausência total de banheiro/sanitário é a carência mais severa captada pelo Universo.

---

## 9. Log de decisões

| Data | Decisão | Alternativa rejeitada | Motivo |
|---|---|---|---|
| 2026-04-26 | Estrutura etapas_t0/ + etapas_t1/ + etapa9_validacao.py | Etapas lineares 1-6 (versão anterior) | Versão linear não comportava a dimensão temporal; estrutura dupla reflete os dois regimes de dados disponíveis |
| 2026-04-26 | IPF em 2 níveis (setor + domínio PNADc) | Calibração em 1 nível apenas | Dois níveis garantem consistência com Censo (precisão local) E com PNADc (controle macro amostral) |
| 2026-04-26 | Resolução H3=8 como padrão | H3=9 ou H3=7 | Res=8 (≈460m de raio) é comparável ao tamanho médio de setores urbanos; res=9 amplifica ruído; res=7 perde variação intraurbana |
| 2026-04-26 | Mapeamento canônico FJP→Censo 2022 introduzido como artefato externo | Busca por keywords conceituais no dicionário | A busca por keywords conceituais ("rústico", "adensado") falhou; variáveis usam terminologia técnica IBGE. Mapeamento curado a partir da NT FJP 4/2023 + Nota Metodológica IBGE 06 destrava o problema. |
| 2026-04-26 | Componentes A1 (paredes), B2 (dormitórios) e C (aluguel) confirmados como indisponíveis no Universo agregado | Usar estimativa baseada em variáveis correlatas | Verificação cruzada com IBGE confirmou: essas variáveis pertencem ao questionário da Amostra (12%), ainda não publicado por setor. Sem data oficial de publicação em abril/2026. |
| 2026-04-26 | Etapa 1 muda de "proxy de componentes FJP" para "índice composto de carências habitacionais Universo" | Calcular componentes FJP individualmente | Variáveis A1, B2 e C só estão na Amostra do Censo 2022, ainda não publicada por setor. O índice composto preserva o desenho metodológico geral (calibração em cascata via PNADc) usando apenas dados disponíveis. |
| 2026-04-26 | Pesos do índice ficam para a Etapa 3 (modelo) — não arbitrados a priori | Pesos iguais ou baseados em literatura | Permite que o modelo aprenda a ponderação que melhor correlaciona com o total FJP do domínio PNADc — mais robusto que pesos fixos. |
| 2026-04-27 | Calibração direta 2 passos em vez de IPF iterativo | IPF-2d iterativo (Deming & Stephan 1940) | IPF-2d diverge quando setores sem H3 válidos criam alvos irredutíveis entre os dois níveis. Calibração direta é matematicamente equivalente no caso de município único: Passo 1 distribui o alvo censitário entre H3 do setor (mantendo proporções RF); Passo 2 aplica fator global PNADc/Censo. Converge em 0 iterações. |
| 2026-04-27 | Mapeamento H3→setor salvo como tabela mapeamento_h3_setor_t0 no DuckDB (Etapa 2) | Recalcular a cada execução da Etapa 5 | Mapeamento demora ~30s para calcular (setores_para_h3 com polyfill + vizinhos); persistir evita reprocessamento e disponibiliza pesos para outras etapas. |
| 2026-04-27 | n_domicilios_grade omitido de covariaveis_h3_t0 por bug (gdf_grade_proj inexistente) corrigido para gdf_grade | Corrigir após Etapa 4 já executada | Bug silenciado por try/except na Etapa 2; covariaveis_h3_t0 e deficit_predito_h3_t0 foram re-executados após correção. deficit_estimado_total manteve-se em 14.186 (n_dom_grade era correto, apenas deficit_estimado ficou NaN). |
| 2026-04-27 | Etapa 7 aplica RF de t0 sobre features t1 (em vez de novo RF) para calcular delta_proxy via proxy_t1−proxy_t0 | Treinar novo RF com dados t1 | Dados rotulados t1 inexistem sem Censo t1; reutilizar o RF t0 mantém a escala do proxy e permite comparação direta entre períodos. O diagnóstico LM sobre os deltas serve apenas para comunicação científica, não para predição. |
| 2026-04-27 | IC do delta_proxy (Etapa 7) calculado via std entre árvores RF (±1.645σ → IC 90%) | Bootstrap ou IC analítico | Std entre árvores é nativamente disponível no RF do scikit-learn sem custo computacional adicional; IC 90% adequado para estimativas intraurbanas com alta variância estrutural. |
| 2026-04-27 | Etapa 9 Módulo C: compara soma_h3 com PNADc (não com proxy×n_dom) | Comparar deficit_calibrado com proxy_setor×n_dom_total | Após calibração em 2 passos, deficit_calibrado_h3 é rescalado pelo fator_dominio PNADc; a comparação correta é com o total PNADc, não com o proxy bruto pré-calibração. |
| 2026-04-27 | Etapa 9 Módulo E usa proxy_carencias_setor (taxa) em vez de deficit_calibrado (contagem absoluta) para E1 (FCU) e E3 (correlação lum) | Usar soma deficit por setor | Setores com FCU tendem a ser menores geograficamente → menos domicílios → menos deficit absoluto mesmo com taxa maior. Usar taxa elimina o viés de tamanho de setor. |
| 2026-04-27 | fcu_setor.cod_setor tem sufixo "P" (ex: 270030005000001P); join com deficit_calibrado_h3_t0.cod_setor_dominante usa LEFT(fcu.cod_setor, 15) | Join direto | Formatos divergem: fcu_setor usa código de 16 chars com sufixo de período ("P"); cod_setor_dominante usa 15 dígitos IBGE padrão. |
