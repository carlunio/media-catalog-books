import csv
from pathlib import Path
from typing import Any

from . import books


EXPORT_FIELDS = [
    "id",
    "isbn",
    "titulo",
    "subtitulo",
    "titulo_completo",
    "autor",
    "editorial",
    "anio",
    "idioma",
    "paginas",
    "palabras_clave",
    "cover_path",
    "pipeline_stage",
    "workflow_status",
]


def _join_list(value: Any) -> str:
    if isinstance(value, list):
        return "; ".join(str(item).strip() for item in value if str(item).strip())
    text = str(value or "").strip()
    return text


def export_books_tsv(output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    rows = books.list_books(limit=200000)

    with output_path.open("w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(
            file,
            fieldnames=EXPORT_FIELDS,
            delimiter="\t",
            quoting=csv.QUOTE_MINIMAL,
        )
        writer.writeheader()

        for row in rows:
            catalog = row.get("catalog") if isinstance(row.get("catalog"), dict) else {}

            writer.writerow(
                {
                    "id": row.get("id"),
                    "isbn": catalog.get("isbn") or row.get("isbn"),
                    "titulo": catalog.get("titulo"),
                    "subtitulo": catalog.get("subtitulo"),
                    "titulo_completo": catalog.get("titulo_completo"),
                    "autor": _join_list(catalog.get("autor")),
                    "editorial": catalog.get("editorial"),
                    "anio": catalog.get("anio"),
                    "idioma": _join_list(catalog.get("idioma")),
                    "paginas": catalog.get("paginas"),
                    "palabras_clave": _join_list(catalog.get("palabras_clave")),
                    "cover_path": row.get("cover_path"),
                    "pipeline_stage": row.get("pipeline_stage"),
                    "workflow_status": row.get("workflow_status"),
                }
            )

    return output_path
