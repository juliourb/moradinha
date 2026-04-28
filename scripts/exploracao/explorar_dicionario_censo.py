"""
explorar_dicionario_censo.py

Lê o dicionário de dados dos agregados por setores censitários 2022 e extrai
os grupos de variáveis relevantes para o cálculo do proxy FJP.

Saídas:
    data/processed/dicionarios/mapeamento_censo_fjp.json
    data/processed/dicionarios/mapeamento_censo_fjp.md
"""

from pathlib import Path
import json
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
XLSX = ROOT / "fonte" / "dicionario_de_dados_agregados_por_setores_censitarios_20250417.xlsx"
OUT_DIR = ROOT / "data" / "processed" / "dicionarios"
OUT_DIR.mkdir(parents=True, exist_ok=True)

KEYWORDS = {
    "aluguel": "aluguel",
    "condicao_ocupacao": "condi",     # condição, condicao
    "onus": "nus",                    # ônus, onus
    "cedido": "cedid",
    "proprio": "pr.prio",             # próprio, proprio (regex)
    "rendimento_nominal": "rendimento",
    "renda": "renda",
    "rustico": "r.stic",              # rústico, rustico
    "adensado": "adensad",
    "paredes": "parede",
    "dormitorio": "dormit",
    "especie": "esp.cie",             # espécie, especie
    "comodos": "c.modo",              # cômodo, comodo
    "improvisado": "improvis",
    "coabitacao": "coabit",
    "familia_convivente": "convivente",
    "sem_banheiro": "banheiro",
    "moradores_dom": "moradores",
    "material_construcao": "material",
    "abastecimento": "abastecimento",
    "coleta_lixo": "lixo",
    "total_dom_geral": "Domicílios Particulares",
}

SHEET = "Dicionário não PCT"


def main():
    print(f"Lendo {XLSX.name}...")
    df = pd.read_excel(XLSX, sheet_name=SHEET, dtype=str)
    df.columns = [c.strip() for c in df.columns]

    resultado = {}
    for label, kw in KEYWORDS.items():
        matches = df[df["Descrição"].str.contains(kw, case=False, na=False, regex=True)]
        resultado[label] = [
            {"variavel": row["Variável"], "tema": row["Tema"], "descricao": row["Descrição"]}
            for _, row in matches.iterrows()
        ]
        print(f"  {label:30s}: {len(resultado[label])} vars")

    json_path = OUT_DIR / "mapeamento_censo_fjp.json"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(resultado, f, ensure_ascii=False, indent=2)
    print(f"\nJSON salvo em: {json_path}")

    md_lines = ["# Mapeamento Dicionário Censo 2022 → FJP\n"]
    md_lines.append(f"Fonte: `{XLSX.name}`  \nAba: `{SHEET}`\n")
    for label, vars_list in resultado.items():
        md_lines.append(f"\n## {label} ({len(vars_list)} variáveis)\n")
        if vars_list:
            md_lines.append("| Variável | Tema | Descrição |")
            md_lines.append("|---|---|---|")
            for v in vars_list:
                desc = v["descricao"].replace("|", "\\|")
                tema = v["tema"].replace("|", "\\|")
                md_lines.append(f"| {v['variavel']} | {tema} | {desc} |")
        else:
            md_lines.append("_Nenhuma variável encontrada._")

    md_path = OUT_DIR / "mapeamento_censo_fjp.md"
    with open(md_path, "w", encoding="utf-8") as f:
        f.write("\n".join(md_lines))
    print(f"MD  salvo em: {md_path}")


if __name__ == "__main__":
    main()
