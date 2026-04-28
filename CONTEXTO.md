# CONTEXTO — Moradinha / PPGPGT-UFABC

Projeto de doutorado em Planejamento e Gestão do Território (PPGPGT-UFABC).

Objetivo: construir estimativas intraurbanas de déficit habitacional multidimensional
por hexágono H3, combinando microdados do Censo 2022, PNADc e covariáveis territoriais
anuais (luminosidade VIIRS, uso do solo MapBiomas, endereços CNEFE, FCU), para
municípios brasileiros sem levantamento específico.

---

## Estrutura do projeto

```
moradinha/
├── modulo_coleta/          # Coleta e persistência de dados brutos
│   ├── orquestrador.py     # coletar_municipio() — ponto de entrada
│   ├── grupos/             # Uma fonte de dados por grupo
│   │   ├── grupo1_geometrias.py
│   │   ├── grupo2_censo.py
│   │   ├── grupo3_logradouros.py
│   │   ├── grupo4_luminosidade.py
│   │   ├── grupo5_pnadc.py
│   │   └── grupo6_uso_solo_precariedade.py
│   ├── utils/              # db_utils, ibge_ftp, osmx, raster_utils
│   └── r_scripts/          # [deprecated] extrair_pnadc.R (substituído por Python)
│
├── modulo_estimacao/       # Estimação espacial e temporal do déficit
│   ├── orquestrador.py     # estimar_municipio() — ponto de entrada
│   ├── etapas_t0/          # Sub-pipeline ano-base (Censo disponível)
│   │   ├── etapa1_proxy_setor.py       # Proxy FJP via Censo 2022
│   │   ├── etapa2_covariaveis_t0.py    # Matriz de covariáveis territoriais
│   │   ├── etapa3_modelo_espacial.py   # Ajuste RF/LM/GWR
│   │   ├── etapa4_predicao_h3_t0.py    # Projeção para grade H3
│   │   └── etapa5_calibracao_t0.py     # IPF (setor + domínio PNADc)
│   ├── etapas_t1/          # Sub-pipeline ano-corrente (sem Censo)
│   │   ├── etapa6_covariaveis_t1.py    # Recalcula cov. anuais + deltas
│   │   ├── etapa7_modelo_temporal.py   # Δdéficit ~ Δlum + Δuso
│   │   └── etapa8_predicao_h3_t1.py    # H3 t1 + calibração PNADc t1
│   ├── etapa9_validacao.py             # CV, Moran's I, comparação FJP
│   └── utils/              # deficit_fjp_proxy, ipf, covariaveis_h3
│
├── data/
│   ├── raw/
│   │   ├── tiles_globais/              # VIIRS e MapBiomas globais (manual)
│   │   │   ├── VNL_v22_..._2022_...average_masked.dat
│   │   │   ├── VNL_npp_2024_...average_masked.dat
│   │   │   ├── brazil_coverage_2022.tif
│   │   │   └── brazil_coverage_2024.tif
│   │   └── {UF}_{municipio}/          # dados brutos por município
│   └── processed/
│       └── {UF}_{municipio}/
│           └── {municipio}.duckdb     # banco por município (todas as tabelas)
│
├── fonte/                  # Dados de referência estáticos versionados
│   ├── grade_estatistica/  # Grade IBGE 50km, 100km, 500km, id58
│   └── 27_AL_Alagoas/      # Áreas de ponderação Alagoas (shapefile)
│
├── CONTEXTO.md             # Este arquivo
├── README.md
└── requirements.txt
```
## Regras de eficiência de execução

Estas regras aplicam-se a TODAS as sessões com o Claude Code no projeto moradinha.
Existem para preservar tokens e tornar a execução estável no Windows + Git Bash.

### R1. Não usar `python -c` com código multilinha
Heredoc inline é frágil no bash do Windows (escape de aspas, quebras de linha,
auto-format do terminal). Sempre criar um arquivo .py em `scripts/exploracao/`
ou `scripts/utils/` e executar. Permitido apenas para comandos triviais de
uma linha (ex: `python -c "import pandas; print(pandas.__version__)"`).

### R2. Exploração de dicionários, schemas e metadados — uma vez, persistido
Toda vez que precisar inspecionar um arquivo grande (XLSX de dicionário, schema
de microdado, metadados do IBGE), o resultado da inspeção deve virar um artefato
em `data/processed/dicionarios/` ou `data/processed/schemas/` (JSON + MD).
Consultas subsequentes leem o artefato, não o arquivo bruto.

Exemplos de artefatos esperados:
- `data/processed/dicionarios/mapeamento_censo_fjp.json` — keywords FJP →
  variáveis Censo 2022
- `data/processed/dicionarios/dicionario_pnadc_anual_v1_{ano}.json` — variáveis
  PNADc com posição, tamanho e descrição (já existe da sessão Grupo 5)
- `data/processed/schemas/duckdb_tabelas.json` — schema de cada tabela do DuckDB
  do município ativo

### R3. Agregação de buscas
Quando precisar aplicar várias buscas no mesmo dataset (ex: 12 keywords no
dicionário do Censo), agrupar TODAS em uma única execução. Nunca rodar 12
comandos separados.

### R4. Output limitado
Comandos que retornam muitas linhas (read_excel, head, glob de muitos arquivos)
devem ter output truncado/resumido na saída. Se precisar do conteúdo completo,
salvar em arquivo e reportar apenas tamanho/path/sumário.

Pattern aceitável:
    df = pd.read_excel(...)
    df.to_json('data/processed/.../arquivo.json')
    print(f"Salvo: {len(df)} linhas em data/processed/.../arquivo.json")

Pattern proibido (em sessão de chat):
    df = pd.read_excel(...)
    print(df.to_string())  # despeja milhares de linhas no contexto

### R5. Consulta a artefatos persistidos antes de explorar de novo
Antes de inspecionar um arquivo bruto, verificar se já existe um artefato
correspondente em `data/processed/dicionarios/` ou `data/processed/schemas/`.
Se existir e estiver atualizado, ler o artefato. Só re-explorar o bruto se
o artefato não cobrir a necessidade atual.

### R6. Nada de heredocs com escape duplo no Windows
Sequências como `\\n`, `\"`, `'\\\"'` indicam que algo está sendo over-escapado
para sobreviver ao terminal. Isso é sinal de que o caminho deveria ser arquivo
.py em disco, não comando inline.
---

## Referência cruzada: coleta → estimação

| Tabela no DuckDB | Produzida por | Consumida por |
|---|---|---|
| `limite_municipal` | Grupo 1 | Grupos 3, 4, 6; Etapa 2 |
| `setores_censitarios` | Grupo 1 | Grupos 2, 6; Etapas 1, 2, 3, 4 |
| `grade_estatistica` | Grupo 1 | Etapas 2, 4 |
| `domicilio01`, `domicilio02` | Grupo 2 | Etapa 1 |
| `responsavel01`, `responsavel03` | Grupo 2 | Etapas 1, 2 |
| `enderecos_cnefe_residencial` | Grupo 3 | Etapa 2 (covariável 4) |
| `enderecos_cnefe_naoresidencial` | Grupo 3 | Etapa 2 (covariável 5) |
| `eixos_osm` | Grupo 3 | Etapa 2 (dist_centro_m), Etapa 9 (Moran's I) |
| `luminosidade_{ano}` | Grupo 4 | Etapas 2, 6 |
| `pnadc_deficit_componentes` | Grupo 5 | Etapas 5, 8 |
| `mapbiomas_{ano}` | Grupo 6 | Etapas 2, 6 |
| `fcu_setor` | Grupo 6 | Etapa 2 (covariáveis 9, 10) |

---

## Fluxo por sessão

```
modulo_coleta.coletar_municipio("2700300", anos=[2022, 2024])
    ↓
modulo_estimacao.estimar_municipio(
    "2700300", ano_t0=2022, ano_t1=2024, resolucao_h3=8,
    db_path=Path("data/processed/AL_Arapiraca/arapiraca.duckdb")
)
    ↓
visualização em QGIS / notebook / exportação GeoPackage
```

---

## Municípios testados

| Código IBGE | Município | UF | Grupos testados | Observações |
|---|---|---|---|---|
| 2701407 | Campo Alegre | AL | 1, 2 | Município pequeno, teste inicial |
| 2700300 | Arapiraca | AL | 1–6 | Município-piloto principal; todos os grupos validados |

---

## Metodologia base

- **Déficit habitacional**: metodologia FJP 2021, 4 componentes (precárias, coabitação, ônus, adensamento)
- **Estimação em pequenas áreas (SAE)**: Fay-Herriot adaptado + Random Forest espacial
- **Inspiração temporal**: BID (2019) "Housing Deficit in LAC" — regressão luminosidade × déficit
- **Calibração**: Iterative Proportional Fitting (IPF) em dois níveis (setor + domínio PNADc)
- **Grade de saída**: Uber H3 resolução 8 (≈460m de raio por hexágono)

---

## Tiles globais (download manual)

Os arquivos abaixo não são versionados no repositório por tamanho (> 10 GB).
O pesquisador os baixa manualmente e os mantém em `data/raw/tiles_globais/`:

| Arquivo | Fonte | Instruções |
|---|---|---|
| `VNL_*_2022_*average_masked.dat` | EOG / Colorado School of Mines | Conta gratuita em eogdata.mines.edu; produto VNL V2.2 Annual |
| `VNL_*_2024_*average_masked.dat` | EOG / Colorado School of Mines | Idem, ano 2024 |
| `brazil_coverage_2022.tif` | MapBiomas Coleção 10 | brasil.mapbiomas.org — Coleção 10, produto de classificação |
| `brazil_coverage_2024.tif` | MapBiomas Coleção 10 | Idem, ano 2024 |

---

## Estado atual (2026-04-26)

| Módulo | Status |
|---|---|
| `modulo_coleta` — Grupos 1–6 | ✅ Completo (validado com Arapiraca) |
| `modulo_estimacao` — Etapa 1 | ✅ Implementada e testada (Arapiraca: 426 setores, proxy_medio=0.19) |
| `modulo_estimacao` — Etapas 2–9 | ⬜ Stubs — implementação em progresso |

Próxima etapa: Etapa 2 (covariaveis_t0) — matriz de 12 covariáveis territoriais por setor.
