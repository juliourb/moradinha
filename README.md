# Moradinha

Pipeline de coleta e processamento de dados territoriais para estimação multidimensional de necessidades habitacionais em municípios brasileiros.

Desenvolvido no âmbito do doutorado em Planejamento e Gestão do Território (PPGPGT – UFABC).

---

## Contexto

O projeto constrói um índice espacial intraurbano baseado em covariáveis territoriais para métodos de desagregação e estimação em pequenas áreas (SAE). O módulo de coleta é a primeira etapa do pipeline: baixa, recorta e persiste dados brutos de qualquer município brasileiro, deixando-os prontos para as etapas de processamento e modelagem.

---

## Estrutura

```
moradinha/
├── modulo_coleta/          # Pipeline de coleta de dados
│   ├── orquestrador.py     # Ponto de entrada (em desenvolvimento)
│   ├── grupos/             # Um módulo por fonte de dados
│   │   ├── grupo1_geometrias.py    # Limites, setores, grade, áreas de ponderação
│   │   ├── grupo2_censo.py         # Censo 2022 — domicílios e renda
│   │   ├── grupo3_logradouros.py   # CNEFE, faces de logradouro, OSM
│   │   ├── grupo4_luminosidade.py  # Luminosidade noturna VIIRS
│   │   ├── grupo5_pnadc.py         # PNADc via R
│   │   └── grupo6_extensoes.py     # Extensões (em desenvolvimento)
│   ├── utils/              # Utilitários compartilhados
│   │   ├── db_utils.py     # Leitura/escrita DuckDB
│   │   ├── ibge_ftp.py     # Download FTP IBGE
│   │   ├── osmx.py         # Download eixos OSM
│   │   └── raster_utils.py # Clip e zonal stats raster
│   └── r_scripts/
│       └── extrair_pnadc.R # Estimativas PNADc com survey design
├── data/
│   ├── raw/                # Dados brutos por município (não versionados)
│   └── processed/          # DuckDB por município (não versionados)
├── fonte/                  # Dados de referência estáticos
│   ├── grade_estatistica/  # Grades IBGE 50km, 100km, 500km, id58
│   └── 27_AL_Alagoas/      # Shapefiles de áreas de ponderação (Alagoas)
└── cache/                  # Cache de downloads (não versionado)
```

---

## Fontes de dados

| Grupo | Fonte | Como obter |
|---|---|---|
| 1 — Geometrias | IBGE FTP + `geobr` | Baixado automaticamente pelo grupo1 |
| 2 — Censo 2022 | IBGE FTP (Agregados por Setor) | Baixado automaticamente pelo grupo2 |
| 3 — Logradouros | IBGE CNEFE + OSMnx | Baixado automaticamente pelo grupo3 |
| 4 — Luminosidade noturna | EOG / Colorado School of Mines | **Download manual** — ver instruções abaixo |
| 5 — PNADc | IBGE via pacote R `PNADcIBGE` | Baixado automaticamente pelo grupo5 via R |

### Luminosidade noturna VIIRS (grupo 4)

O tile global (~11 GB) não é versionado no repositório. Para obter:

1. Criar conta gratuita em: https://eogdata.mines.edu/products/vnl/
2. Baixar o arquivo anual (ex: 2022):
   - Produto: **VNL V2.2 Annual**
   - Arquivo: `VNL_v22_npp-j01_2022_global_vcmslcfg_c202303062300.average_masked.dat.tif`
   - URL base: `https://eogdata.mines.edu/nighttime_light/annual/v22/2022/`
3. Salvar em: `data/raw/tiles_globais/`
4. Licença: **CC BY 4.0** — requer token de autenticação EOG (gratuito)

---

## Uso

```python
from modulo_coleta.grupos.grupo1_geometrias import coletar_grupo1
from modulo_coleta.grupos.grupo2_censo import coletar_grupo2
# ... etc

# Exemplo: coletar geometrias de Campo Alegre-AL
coletar_grupo1("2701407", base_dir=Path("data"))
```

O orquestrador unificado (`coletar_municipio`) está em desenvolvimento.

---

## Dependências

**Python:** `geopandas`, `duckdb`, `rasterio`, `rasterstats`, `osmnx`, `geobr`, `pandas`, `numpy`

**R:** `PNADcIBGE`, `survey`, `geobr` (necessário apenas para o grupo 5)

---

## Estado de implementação

| Componente | Status |
|---|---|
| `utils/db_utils.py` | ✅ Concluído |
| `utils/ibge_ftp.py` | ✅ Concluído |
| `utils/osmx.py` | ✅ Concluído |
| `utils/raster_utils.py` | ✅ Concluído |
| `grupo1_geometrias.py` | ✅ Concluído |
| `grupo2_censo.py` | ✅ Concluído |
| `grupo3_logradouros.py` | ✅ Concluído |
| `grupo4_luminosidade.py` | ✅ Concluído |
| `grupo5_pnadc.py` | ✅ Concluído |
| `grupo6_extensoes.py` | ⬜ Em desenvolvimento |
| `orquestrador.py` | ⬜ Em desenvolvimento |
