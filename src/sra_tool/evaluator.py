from __future__ import annotations
from typing import Any
from .constants import ADVANCE_ALLOWED
from .models import StageEvaluation

def _prompt_non_empty(message: str) -> str:
    while True:
        value = input(message).strip()
        if value:
            return value
        print("Entrada obligatoria. Intente nuevamente.")

def _prompt_yes_no(message: str) -> bool:
    while True:
        value = input(f"{message} [s/n]: ").strip().lower()
        if value in {"s", "si", "sí", "y", "yes"}:
            return True
        if value in {"n", "no"}:
            return False
        print("Respuesta no válida. Use s/n.")

def evaluate_strategy_stage(state: Any) -> StageEvaluation:
    evidence = {
        "justificacion": _prompt_non_empty(
            "Explique la justificación metodológica de la estrategia seleccionada: "
        ),
        "confirma_adecuacion": _prompt_yes_no(
            "¿Confirma que la estrategia seleccionada es coherente con el objetivo de la revisión?"
        ),
    }
    criteria = [
        "Debe existir una estrategia válida.",
        "La justificación metodológica no puede estar vacía.",
        "Debe confirmarse coherencia entre estrategia y objetivo.",
    ]
    findings: list[str] = []
    observations: list[str] = []
    warnings: list[str] = []
    if not state.strategy:
        findings.append("No existe estrategia definida.")
        decision = "rechazado"
    elif not evidence["justificacion"].strip():
        findings.append("La justificación está vacía.")
        decision = "revision requerida"
    elif not evidence["confirma_adecuacion"]:
        findings.append("No se confirmó la adecuación metodológica.")
        decision = "revision requerida"
    else:
        findings.append(f"Estrategia registrada: {state.strategy}.")
        if len(evidence["justificacion"]) < 20:
            observations.append("La justificación existe, pero es breve.")
            decision = "aprobado con observaciones"
        else:
            decision = "aprobado"
    return StageEvaluation(
        stage_name="strategy_definition",
        criteria=criteria,
        evidence_requested=list(evidence.keys()),
        evidence_received=evidence,
        findings=findings,
        observations=observations,
        warnings=warnings,
        decision=decision,
        allow_advance=decision in ADVANCE_ALLOWED,
    )

def evaluate_capture_stage(state: Any) -> StageEvaluation:
    evidence = {
        "confirma_pertinencia_tematica": _prompt_yes_no(
            "¿Confirma que los resultados recuperados son temáticamente pertinentes?"
        ),
        "observacion_sobre_volumen": _prompt_non_empty(
            "Describa si el volumen recuperado es razonable para la revisión: "
        ),
    }
    criteria = [
        "La consulta o URL no puede estar vacía.",
        "Debe existir al menos un registro recuperado.",
        "Deben registrarse páginas descargadas y total recuperado.",
        "Debe confirmarse pertinencia temática.",
    ]
    findings: list[str] = []
    observations: list[str] = []
    warnings: list[str] = []
    if not state.openalex_user_input:
        findings.append("No se registró la consulta ni URL de captura.")
        decision = "rechazado"
    elif state.openalex_records_retrieved <= 0:
        findings.append("No se recuperaron registros.")
        decision = "rechazado"
    elif state.openalex_pages_downloaded <= 0:
        findings.append("No se registraron páginas descargadas.")
        decision = "revision requerida"
    elif not evidence["confirma_pertinencia_tematica"]:
        findings.append("La pertinencia temática no fue confirmada.")
        decision = "revision requerida"
    else:
        findings.append(
            f"Se recuperaron {state.openalex_records_retrieved} registros en "
            f"{state.openalex_pages_downloaded} páginas."
        )
        if state.openalex_records_retrieved < 10:
            warnings.append("Volumen recuperado bajo; revisar sensibilidad de búsqueda.")
            decision = "aprobado con observaciones"
        elif state.openalex_records_retrieved > 50000:
            warnings.append(
                "Volumen recuperado muy alto; revisar especificidad antes de screening."
            )
            decision = "aprobado con observaciones"
        else:
            decision = "aprobado"
    return StageEvaluation(
        stage_name="openalex_capture",
        criteria=criteria,
        evidence_requested=list(evidence.keys()),
        evidence_received=evidence,
        findings=findings,
        observations=observations,
        warnings=warnings,
        decision=decision,
        allow_advance=decision in ADVANCE_ALLOWED,
    )

def evaluate_harmonization_stage(state: Any, summary: dict[str, Any]) -> StageEvaluation:
    evidence = {
        "confirma_revisar_campos_na": _prompt_yes_no(
            "¿Confirma que revisó y acepta los campos sin correspondencia directa como NA?"
        ),
        "comentario_de_mapeo": _prompt_non_empty(
            "Describa cualquier limitación o decisión relevante del mapeo: "
        ),
    }
    criteria = [
        "El número de registros armonizados debe ser mayor que cero.",
        "Las columnas deben coincidir exactamente con el esquema objetivo.",
        "Debe declararse aceptación informada de campos NA/no mapeados.",
    ]
    findings: list[str] = []
    observations: list[str] = []
    warnings: list[str] = []
    if summary.get("record_count", 0) <= 0:
        findings.append("No existen registros armonizados.")
        decision = "rechazado"
    elif not summary.get("schema_ok", False):
        findings.append("El esquema armonizado no coincide con las columnas objetivo.")
        decision = "rechazado"
    elif not evidence["confirma_revisar_campos_na"]:
        findings.append("No se confirmó revisión de campos NA/no mapeados.")
        decision = "revision requerida"
    else:
        findings.append(
            f"Armonización completada para {summary.get('record_count', 0)} registros."
        )
        critical_empty_rate = summary.get("critical_empty_rate", 1.0)
        if critical_empty_rate > 0.4:
            warnings.append(
                "Alta proporción de vacíos en campos críticos tras armonización."
            )
            decision = "aprobado con observaciones"
        else:
            decision = "aprobado"
    return StageEvaluation(
        stage_name="harmonization",
        criteria=criteria,
        evidence_requested=list(evidence.keys()),
        evidence_received=evidence,
        findings=findings,
        observations=observations,
        warnings=warnings,
        decision=decision,
        allow_advance=decision in ADVANCE_ALLOWED,
    )

def evaluate_export_stage(state: Any, summary: dict[str, Any]) -> StageEvaluation:
    evidence = {
        "confirma_ruta_salida": _prompt_yes_no(
            "¿Confirma que la ruta de salida es correcta y portable para el proyecto?"
        ),
        "nota_exportacion": _prompt_non_empty(
            "Indique cualquier observación sobre el archivo exportado: "
        ),
    }
    criteria = [
        "El archivo CSV debe existir.",
        "El archivo debe poder leerse.",
        "La cantidad de filas debe ser coherente con los registros armonizados.",
    ]
    findings: list[str] = []
    observations: list[str] = []
    warnings: list[str] = []
    if not state.exported_csv_path:
        findings.append("No existe ruta de exportación registrada.")
        decision = "rechazado"
    elif summary.get("rows", -1) < 0:
        findings.append("No se pudo validar el archivo exportado.")
        decision = "rechazado"
    elif summary.get("rows") != len(state.harmonized_records):
        findings.append("El número de filas del CSV no coincide con los registros armonizados.")
        decision = "revision requerida"
    elif not evidence["confirma_ruta_salida"]:
        findings.append("La ruta de salida no fue confirmada.")
        decision = "revision requerida"
    else:
        findings.append(
            f"CSV validado con {summary.get('rows')} filas y {summary.get('columns')} columnas."
        )
        decision = "aprobado"
    return StageEvaluation(
        stage_name="csv_export",
        criteria=criteria,
        evidence_requested=list(evidence.keys()),
        evidence_received=evidence,
        findings=findings,
        observations=observations,
        warnings=warnings,
        decision=decision,
        allow_advance=decision in ADVANCE_ALLOWED,
    )

def evaluate_corpus_stage(summary: dict[str, Any]) -> StageEvaluation:
    evidence = {
        "confirma_listo_para_siguiente_fase": _prompt_yes_no(
            "¿Confirma que el corpus es suficiente para pasar a deduplicación o screening?"
        ),
        "comentario_calidad": _prompt_non_empty(
            "Describa su evaluación cualitativa del corpus: "
        ),
    }
    criteria = [
        "Debe existir al menos un registro.",
        "La completitud de título y año debe ser alta.",
        "El corpus no debe presentar debilidad crítica en metadatos básicos.",
    ]
    findings: list[str] = []
    observations: list[str] = []
    warnings: list[str] = []
    total = summary.get("total_records", 0)
    title_rate = summary.get("title_completion_rate", 0.0)
    year_rate = summary.get("year_completion_rate", 0.0)
    doi_rate = summary.get("doi_completion_rate", 0.0)
    abstract_rate = summary.get("abstract_completion_rate", 0.0)
    if total <= 0:
        findings.append("El corpus está vacío.")
        decision = "rechazado"
    elif title_rate < 0.95 or year_rate < 0.95:
        findings.append("La completitud de título o año es insuficiente.")
        decision = "rechazado"
    elif not evidence["confirma_listo_para_siguiente_fase"]:
        findings.append("No se confirmó preparación para la siguiente fase.")
        decision = "revision requerida"
    else:
        findings.append(f"Corpus evaluado con {total} registros.")
        if doi_rate < 0.50:
            warnings.append("Baja completitud de DOI; la deduplicación por DOI será limitada.")
        if abstract_rate < 0.40:
            warnings.append("Baja completitud de abstract; el screening textual puede verse afectado.")
        decision = "aprobado con observaciones" if warnings else "aprobado"
    return StageEvaluation(
        stage_name="corpus_validation",
        criteria=criteria,
        evidence_requested=list(evidence.keys()),
        evidence_received=evidence,
        findings=findings,
        observations=observations,
        warnings=warnings,
        decision=decision,
        allow_advance=decision in ADVANCE_ALLOWED,
    )
