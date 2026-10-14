from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

OpenAIEndpoint = Literal["responses", "chat_completions"]
OpenAIImageDetail = Literal["low", "high", "original", "auto"]


@dataclass(frozen=True)
class OpenAIModelProfile:
    endpoint: OpenAIEndpoint
    reasoning_effort: str | None
    temperature: float | None
    structured_outputs: bool
    label: str
    vision_detail: OpenAIImageDetail = "auto"


DEFAULT_OPENAI_CATALOG_MODEL = "gpt-6-luna"
DEFAULT_OPENAI_OCR_MODEL = "gpt-6-luna"

# Estas son las opciones que se muestran para catalogar. Cada una tiene una
# configuración probada; el backend mantiene perfiles adicionales para poder
# abrir configuraciones antiguas sin volver a aplicar parámetros incompatibles.
OPENAI_CATALOG_MODEL_OPTIONS: tuple[str, ...] = (
    "gpt-6-luna",
    "gpt-6.1-sol",
    "gpt-4o-mini",
)

# El OCR local sigue siendo el proveedor predeterminado de la aplicación. Estas
# son las opciones remotas disponibles cuando se elige OpenAI expresamente.
OPENAI_OCR_MODEL_OPTIONS: tuple[str, ...] = (
    "gpt-6-luna",
    "gpt-6.1-sol",
)


OPENAI_MODEL_PROFILES: dict[str, OpenAIModelProfile] = {
    "gpt-6.1-sol": OpenAIModelProfile(
        endpoint="responses",
        reasoning_effort="low",
        temperature=None,
        structured_outputs=True,
        label="GPT-6.1 Sol · mayor calidad · razonamiento bajo",
        vision_detail="original",
    ),
    "gpt-6-astra": OpenAIModelProfile(
        endpoint="responses",
        reasoning_effort="low",
        temperature=None,
        structured_outputs=True,
        label="GPT-6 Astra · máxima calidad",
        vision_detail="original",
    ),
    "gpt-6-sol": OpenAIModelProfile(
        endpoint="responses",
        reasoning_effort="none",
        temperature=0.0,
        structured_outputs=True,
        label="GPT-6 Sol · compatibilidad",
        vision_detail="original",
    ),
    "gpt-6-luna": OpenAIModelProfile(
        endpoint="responses",
        reasoning_effort="low",
        temperature=None,
        structured_outputs=True,
        label="GPT-6 Luna · recomendado · razonamiento bajo",
        vision_detail="original",
    ),
    "gpt-4o-mini": OpenAIModelProfile(
        endpoint="responses",
        reasoning_effort=None,
        temperature=0.0,
        structured_outputs=True,
        label="GPT-4o mini · opción anterior económica",
        vision_detail="high",
    ),
    "gpt-4o": OpenAIModelProfile(
        endpoint="responses",
        reasoning_effort=None,
        temperature=0.0,
        structured_outputs=True,
        label="GPT-4o · opción anterior",
        vision_detail="high",
    ),
    "gpt-4-turbo": OpenAIModelProfile(
        endpoint="chat_completions",
        reasoning_effort=None,
        temperature=0.0,
        structured_outputs=False,
        label="GPT-4 Turbo · heredado",
    ),
}


LEGACY_OPENAI_MODEL_PROFILE = OpenAIModelProfile(
    endpoint="chat_completions",
    reasoning_effort=None,
    temperature=0.0,
    structured_outputs=False,
    label="Modelo OpenAI personalizado · compatibilidad",
)


def get_openai_model_profile(model: str) -> OpenAIModelProfile:
    """Devuelve la configuración compatible con un modelo o snapshot conocido."""

    normalized = str(model or "").strip().lower()
    direct = OPENAI_MODEL_PROFILES.get(normalized)
    if direct is not None:
        return direct

    # Los snapshots suelen añadir una fecha al alias estable.
    for model_name, profile in OPENAI_MODEL_PROFILES.items():
        if normalized.startswith(f"{model_name}-"):
            return profile

    return LEGACY_OPENAI_MODEL_PROFILE


def openai_model_label(model: str) -> str:
    profile = get_openai_model_profile(model)
    if model in OPENAI_MODEL_PROFILES:
        return profile.label
    return f"{model} · configuración heredada"


def openai_ocr_model_label(model: str) -> str:
    normalized = str(model or "").strip().lower()
    if normalized == "gpt-6-luna":
        return "GPT-6 Luna · recomendado para OCR · económico"
    if normalized == "gpt-6.1-sol":
        return "GPT-6.1 Sol · más calidad · mayor coste"
    return openai_model_label(model)
