"""
orquestrador.py — Ponto de entrada do módulo de estimação.

Executa o pipeline completo (ou parcial) de estimação do déficit habitacional
em dois sub-períodos:

    t0 (ano-base):    Etapas 1-5  — requer Censo 2022 + todas as fontes do modulo_coleta
    t1 (ano-corrente): Etapas 6-8 — requer t0 completo + VIIRS t1 + MapBiomas t1
    validação:         Etapa 9    — transversal, executa após t0 e/ou t1

Uso
───
    from pathlib import Path
    from modulo_estimacao.orquestrador import estimar_municipio

    estimar_municipio(
        codigo_ibge="2700300",
        ano_t0=2022,
        ano_t1=2024,
        resolucao_h3=8,
        db_path=Path("data/processed/AL_Arapiraca/arapiraca.duckdb"),
        etapas=[1, 2, 3, 4, 5, 6, 7, 8, 9],  # default: todas
        modelo_t0="rf",
        modelo_temporal="lm",
    )

Pré-requisito
─────────────
    DuckDB do município deve ter sido populado pelo modulo_coleta:
    Grupos 1, 2, 3, 4, 5 e 6 completos para ano_t0.
    Grupos 4 e 6 completos para ano_t1 (para etapas 6-8).
"""

from __future__ import annotations

import logging
from pathlib import Path

logger = logging.getLogger(__name__)

_ETAPAS_T0 = frozenset({1, 2, 3, 4, 5})
_ETAPAS_T1 = frozenset({6, 7, 8})
_ETAPA_VALIDACAO = frozenset({9})
_TODAS_ETAPAS = _ETAPAS_T0 | _ETAPAS_T1 | _ETAPA_VALIDACAO


def _importar_etapas():
    """Imports lazy para evitar overhead quando só parte do pipeline é usada."""
    from modulo_estimacao.etapas_t0.etapa1_proxy_setor import calcular_proxy_setor
    from modulo_estimacao.etapas_t0.etapa2_covariaveis_t0 import (
        extrair_covariaveis_setor_t0,
        extrair_covariaveis_h3_t0,
    )
    from modulo_estimacao.etapas_t0.etapa3_modelo_espacial import ajustar_modelo_t0
    from modulo_estimacao.etapas_t0.etapa4_predicao_h3_t0 import predizer_h3_t0
    from modulo_estimacao.etapas_t0.etapa5_calibracao_t0 import calibrar_h3_t0
    from modulo_estimacao.etapas_t1.etapa6_covariaveis_t1 import extrair_covariaveis_setor_t1
    from modulo_estimacao.etapas_t1.etapa7_modelo_temporal import ajustar_modelo_temporal
    from modulo_estimacao.etapas_t1.etapa8_predicao_h3_t1 import predizer_h3_t1
    from modulo_estimacao.etapa9_validacao import validar_estimativas

    return {
        1: calcular_proxy_setor,
        2: (extrair_covariaveis_setor_t0, extrair_covariaveis_h3_t0),
        3: ajustar_modelo_t0,
        4: predizer_h3_t0,
        5: calibrar_h3_t0,
        6: extrair_covariaveis_setor_t1,
        7: ajustar_modelo_temporal,
        8: predizer_h3_t1,
        9: validar_estimativas,
    }


def _checar_pulos(etapas_a_rodar: set[int], tabelas_presentes: set[str]) -> set[int]:
    """
    Identifica etapas que já têm saídas salvas e podem ser puladas (se forcar=False).
    """
    SAIDAS = {
        1: "proxy_setor",
        2: "covariaveis_h3_t0",
        3: "modelo_t0_diagnostico",
        4: "deficit_predito_h3_t0",
        5: "deficit_calibrado_h3_t0",
        6: "delta_covariaveis_setor",
        7: "delta_proxy_setor_predito",
        8: "deficit_calibrado_h3_t1",
        9: "validacao_resumo",
    }
    return {e for e in etapas_a_rodar if SAIDAS.get(e) in tabelas_presentes}


def estimar_municipio(
    codigo_ibge: str,
    ano_t0: int,
    ano_t1: int,
    db_path: Path,
    resolucao_h3: int = 8,
    etapas: list[int] | None = None,
    modelo_t0: str = "rf",
    modelo_temporal: str = "lm",
    output_dir: Path | None = None,
    forcar: bool = False,
) -> dict:
    """
    Executa o pipeline de estimação para um município.

    Parâmetros
    ----------
    codigo_ibge : str
        Código IBGE de 7 dígitos.
    ano_t0 : int
        Ano-base com Censo disponível (ex: 2022).
    ano_t1 : int
        Ano-corrente (ex: 2024). Requer VIIRS e MapBiomas de ano_t1 no DuckDB.
    db_path : Path
        Caminho para o DuckDB já populado pelo modulo_coleta.
    resolucao_h3 : int
        Resolução H3 (8 recomendado para setores urbanos médios).
    etapas : list[int] | None
        Etapas a executar (1-9). Se None, executa todas.
    modelo_t0 : str
        Algoritmo para Etapa 3: 'rf' (padrão) ou 'lm' ou 'gwr'.
    modelo_temporal : str
        Algoritmo para Etapa 7: 'lm' (padrão) ou 'rf'.
    output_dir : Path | None
        Pasta para modelos serializados. Default: db_path.parent.
    forcar : bool
        Se True, re-executa etapas mesmo que já concluídas.

    Retorna
    -------
    dict
        {"status": "ok"|"erro", "etapas_executadas": [...], "etapas_ok": [...],
         "etapas_erro": [...], "resultados": {etapa: dict}, "mensagem": "..."}
    """
    from modulo_coleta.utils.db_utils import abrir_conexao

    db_path = Path(db_path)
    if not db_path.exists():
        return {
            "status": "erro",
            "mensagem": f"DuckDB não encontrado: {db_path}",
            "etapas_executadas": [],
            "etapas_ok": [],
            "etapas_erro": [],
        }

    etapas_a_rodar = sorted(set(etapas) if etapas is not None else _TODAS_ETAPAS)
    invalidas = set(etapas_a_rodar) - _TODAS_ETAPAS
    if invalidas:
        return {
            "status": "erro",
            "mensagem": f"Etapas inválidas: {sorted(invalidas)}. Válidas: 1-9.",
            "etapas_executadas": [],
            "etapas_ok": [],
            "etapas_erro": [],
        }

    output_dir = Path(output_dir) if output_dir else db_path.parent

    logger.info(
        "[Orquestrador] municipio=%s | t0=%d | t1=%d | res_h3=%d | etapas=%s | forcar=%s",
        codigo_ibge, ano_t0, ano_t1, resolucao_h3, etapas_a_rodar, forcar,
    )

    fns = _importar_etapas()
    conn = abrir_conexao(db_path)

    try:
        tabelas_presentes = {r[0] for r in conn.execute("SHOW TABLES").fetchall()}
        ja_prontas = _checar_pulos(set(etapas_a_rodar), tabelas_presentes) if not forcar else set()
        if ja_prontas and not forcar:
            logger.info("[Orquestrador] Etapas com saídas já salvas (serão puladas): %s", sorted(ja_prontas))

        executadas, ok, erros, resultados = [], [], [], {}

        for num in etapas_a_rodar:
            if num in ja_prontas:
                logger.info("[Orquestrador] Etapa %d — PULADA (já existe no DuckDB)", num)
                continue

            logger.info("[Orquestrador] Iniciando Etapa %d...", num)
            executadas.append(num)

            try:
                if num == 1:
                    r = fns[1](codigo_ibge, db_conn=conn)

                elif num == 2:
                    fn_setor, fn_h3 = fns[2]
                    r_s = fn_setor(codigo_ibge, ano_t0=ano_t0, db_conn=conn)
                    if r_s["status"] != "ok":
                        raise RuntimeError(f"Etapa 2 (setor): {r_s.get('mensagem', 'erro')}")
                    r = fn_h3(codigo_ibge, ano_t0=ano_t0, resolucao_h3=resolucao_h3, db_conn=conn)
                    r.setdefault("camadas", [])
                    r["camadas"] = r_s.get("camadas", []) + r["camadas"]

                elif num == 3:
                    r = fns[3](
                        codigo_ibge, ano_t0=ano_t0, db_conn=conn,
                        modelo=modelo_t0, output_dir=output_dir,
                    )

                elif num == 4:
                    r = fns[4](
                        codigo_ibge, ano_t0=ano_t0, resolucao_h3=resolucao_h3,
                        db_conn=conn, output_dir=output_dir,
                    )

                elif num == 5:
                    r = fns[5](codigo_ibge, db_conn=conn)

                elif num == 6:
                    r = fns[6](
                        codigo_ibge, ano_t0=ano_t0, ano_t1=ano_t1, db_conn=conn,
                    )

                elif num == 7:
                    r = fns[7](
                        codigo_ibge, ano_t0=ano_t0, ano_t1=ano_t1,
                        db_conn=conn, modelo=modelo_temporal, output_dir=output_dir,
                    )

                elif num == 8:
                    r = fns[8](
                        codigo_ibge, ano_t0=ano_t0, ano_t1=ano_t1,
                        resolucao_h3=resolucao_h3, db_conn=conn,
                    )

                elif num == 9:
                    r = fns[9](codigo_ibge, db_conn=conn)

                resultados[num] = r
                if r.get("status") == "ok":
                    ok.append(num)
                    logger.info("[Orquestrador] Etapa %d — OK | camadas=%s", num, r.get("camadas", []))
                else:
                    erros.append(num)
                    logger.error(
                        "[Orquestrador] Etapa %d — ERRO: %s", num, r.get("mensagem", "?")
                    )

            except Exception as exc:
                logger.exception("[Orquestrador] Etapa %d — exceção: %s", num, exc)
                erros.append(num)
                resultados[num] = {"status": "erro", "mensagem": str(exc)}

        status_final = "ok" if not erros else ("parcial" if ok else "erro")
        if not erros and not ok:
            msg = f"Nenhuma etapa executada — {len(ja_prontas)} já existiam no DuckDB (forcar=False)."
        elif not erros:
            msg = f"{len(ok)} etapa(s) concluídas com sucesso."
        else:
            msg = f"{len(ok)} ok, {len(erros)} com erro: etapas {erros}."
        logger.info("[Orquestrador] Concluído — status=%s | %s", status_final, msg)

        return {
            "status": status_final,
            "mensagem": msg,
            "etapas_executadas": executadas,
            "etapas_ok": ok,
            "etapas_erro": erros,
            "etapas_puladas": sorted(ja_prontas),
            "resultados": resultados,
        }

    finally:
        conn.close()
