"""
lookup_variaveis_censo.py

Faz lookup de intervalos de variáveis no dicionário do Censo 2022
e também busca por conteúdo específico nas descrições.

Uso:
    python scripts/exploracao/lookup_variaveis_censo.py
"""

from pathlib import Path
import json
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
XLSX = ROOT / "fonte" / "dicionario_de_dados_agregados_por_setores_censitarios_20250417.xlsx"
OUT_DIR = ROOT / "data" / "processed" / "dicionarios"
OUT_DIR.mkdir(parents=True, exist_ok=True)

SHEET = "Dicionário não PCT"

# Intervalos de interesse para FJP
INTERVALOS = [
    ("V00040", "V00089"),   # final de domicilio01 — inclui espécie, condição, moradores
    ("V00090", "V00140"),   # inicio de domicilio02
    ("V00140", "V00180"),   # domicilio02 continuação
    ("V00420", "V00495"),   # final de domicilio02
]

# Busca textual adicional
BUSCAS_TEXTO = [
    "proprio",
    "propr",
    "alug",
    "locat",
    "cond",
    "remuner",
    "rendimento",
    "salario",
    "dormit",
    "rustic",
    "comodo",
    "cubi",
    "moren",
    "familia",
    "nucleo",
    "nucl",
    "secundar",
    "conviv",
]


def varnum(v: str) -> int:
    return int(v[1:])


def main():
    print(f"Lendo {XLSX.name}...")
    df = pd.read_excel(XLSX, sheet_name=SHEET, dtype=str)
    df.columns = [c.strip() for c in df.columns]
    df["_num"] = df["Variável"].str[1:].str.strip().apply(
        lambda x: int(x) if x.isdigit() else -1
    )

    # 1. Lookup por intervalo
    result = {"intervalos": {}, "buscas_texto": {}}
    print("\n=== LOOKUP POR INTERVALO ===")
    for v_ini, v_fim in INTERVALOS:
        n_ini, n_fim = varnum(v_ini), varnum(v_fim)
        sub = df[(df["_num"] >= n_ini) & (df["_num"] <= n_fim)].copy()
        label = f"{v_ini}-{v_fim}"
        result["intervalos"][label] = [
            {"variavel": r["Variável"], "tema": r["Tema"], "descricao": r["Descrição"]}
            for _, r in sub.iterrows()
        ]
        print(f"\n  {label} ({len(sub)} vars):")
        for _, r in sub.head(40).iterrows():
            print(f"    {r['Variável']:8s}  {r['Descrição']}")
        if len(sub) > 40:
            print(f"    ... +{len(sub)-40} mais")

    # 2. Busca textual
    print("\n\n=== BUSCA TEXTUAL ===")
    for kw in BUSCAS_TEXTO:
        hits = df[df["Descrição"].str.lower().str.contains(kw.lower(), na=False)]
        result["buscas_texto"][kw] = [
            {"variavel": r["Variável"], "descricao": r["Descrição"]}
            for _, r in hits.iterrows()
        ]
        if len(hits) > 0:
            print(f"\n  '{kw}' ({len(hits)} vars):")
            for _, r in hits.head(10).iterrows():
                print(f"    {r['Variável']:8s}  {r['Descrição']}")
            if len(hits) > 10:
                print(f"    ... +{len(hits)-10} mais")
        else:
            print(f"  '{kw}': 0 hits")

    # Salva resultado
    out = OUT_DIR / "lookup_variaveis_censo.json"
    with open(out, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    print(f"\nResultado salvo em: {out}")


if __name__ == "__main__":
    main()
