import hashlib
import json
import os
import re
import tempfile
from collections import Counter
from pathlib import Path
from typing import Any

import pillow_heif
from PIL import Image, ImageOps

HEIF_EXTENSIONS = {".heic", ".heif"}
IMAGE_STEM_PATTERN = re.compile(
    r"^(?P<module>\d{1,2})(?P<block>[A-Ca-c])(?P<sequence>\d{1,4})"
    r"(?:_(?P<position>\d+))?$"
)


def image_identity(
    path: str | Path, *, require_canonical: bool = False
) -> tuple[str, int, str] | None:
    """Return ``(book_id, position, canonical_stem)`` for a valid image name."""
    stem = Path(path).stem
    match = IMAGE_STEM_PATTERN.fullmatch(stem)
    if match is None:
        return None

    module_number = int(match.group("module"))
    sequence_number = int(match.group("sequence"))
    position = int(match.group("position") or "1")
    if not 1 <= module_number <= 99 or sequence_number < 1 or position < 1:
        return None

    book_id = (
        f"{module_number:02d}"
        f"{match.group('block').upper()}"
        f"{sequence_number:04d}"
    )
    canonical_stem = book_id if position == 1 else f"{book_id}_{position}"
    if require_canonical and stem != canonical_stem:
        return None
    return book_id, position, canonical_stem


def _files(folder: Path, *, recursive: bool, extensions: set[str]) -> list[Path]:
    candidates = folder.rglob("*") if recursive else folder.glob("*")
    return sorted(
        path
        for path in candidates
        if path.is_file() and path.suffix.lower() in extensions
    )


def _relative(path: Path, base: Path) -> str:
    return str(path.relative_to(base))


def _is_case_only_rename(source: Path, target: Path) -> bool:
    source_text = os.fspath(source)
    target_text = os.fspath(target)
    return source_text != target_text and os.path.normcase(
        source_text
    ) == os.path.normcase(target_text)


def _valid_jpeg(path: Path) -> bool:
    try:
        with Image.open(path) as image:
            if image.format not in {"JPEG", "MPO"}:
                return False
            image.verify()
        return True
    except (OSError, ValueError):
        return False


def _valid_heif(path: Path) -> bool:
    try:
        pillow_heif.register_heif_opener()
        with Image.open(path) as image:
            image.load()
        return True
    except (OSError, ValueError):
        return False


def _fingerprint(
    base: Path,
    files: list[Path],
    *,
    normalize_names: bool,
    convert_heic: bool,
    delete_original_heic: bool,
    recursive: bool,
    extensions: set[str],
) -> str:
    state: list[dict[str, Any]] = []
    for path in files:
        stat = path.stat()
        state.append(
            {
                "path": _relative(path, base),
                "size": stat.st_size,
                "mtime_ns": stat.st_mtime_ns,
            }
        )
    payload = {
        "files": state,
        "normalize_names": normalize_names,
        "convert_heic": convert_heic,
        "delete_original_heic": delete_original_heic,
        "recursive": recursive,
        "extensions": sorted(extensions),
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def _add_issue(
    issues: list[dict[str, Any]],
    *,
    kind: str,
    file: str,
    message: str,
) -> None:
    issues.append({"kind": kind, "file": file, "message": message})


def build_preparation_plan(
    folder: str | Path,
    *,
    recursive: bool,
    extensions: set[str],
    normalize_names: bool,
    convert_heic: bool,
    delete_original_heic: bool,
    expected_block: str | None = None,
    expected_module: str | None = None,
) -> dict[str, Any]:
    """Inspect a module and describe every file operation without changing it."""
    base = Path(folder).resolve()
    scan_extensions = set(extensions) | HEIF_EXTENSIONS | {".jpg", ".jpeg"}
    files = _files(base, recursive=recursive, extensions=scan_extensions)
    actions: list[dict[str, Any]] = []
    issues: list[dict[str, Any]] = []
    virtual_paths: dict[Path, Path] = {path: path for path in files}
    reserved_targets: dict[Path, Path] = {}
    blocked_sources: set[Path] = set()
    blocked_book_ids: set[str] = set()

    for source in files:
        identity = image_identity(source)
        relative_source = _relative(source, base)
        if identity is None:
            blocked_sources.add(source)
            _add_issue(
                issues,
                kind="invalid_name",
                file=relative_source,
                message=(
                    "El nombre no permite identificar el libro. Corrígelo a mano, "
                    "por ejemplo 01A0001.jpg o 01A0001_2.jpg."
                ),
            )
            continue

        book_id, _position, canonical_stem = identity
        image_module, image_block = book_id[:2], book_id[2]
        if (expected_block and image_block != expected_block) or (
            expected_module and image_module != expected_module
        ):
            blocked_sources.add(source)
            blocked_book_ids.add(book_id)
            _add_issue(
                issues,
                kind="scope_mismatch",
                file=relative_source,
                message=(
                    f"El nombre identifica el módulo {image_block}/{image_module}, "
                    f"pero el archivo está en {expected_block}/{expected_module}. "
                    "Muévelo o corrige el nombre a mano."
                ),
            )
            continue

        target = source.with_name(f"{canonical_stem}{source.suffix.lower()}")
        if not normalize_names or source.name == target.name:
            reserved_targets.setdefault(source, source)
            continue

        owner = reserved_targets.get(target)
        case_only_rename = _is_case_only_rename(source, target)
        if (target.exists() and not case_only_rename) or (
            owner is not None and owner != source
        ):
            blocked_book_ids.add(book_id)
            _add_issue(
                issues,
                kind="rename_conflict",
                file=relative_source,
                message=f"El nombre de destino ya existe: {_relative(target, base)}.",
            )
            continue

        reserved_targets[target] = source
        virtual_paths[source] = target
        actions.append(
            {
                "type": "rename",
                "source": relative_source,
                "destination": _relative(target, base),
            }
        )

    virtual_to_source = {virtual: source for source, virtual in virtual_paths.items()}
    converted_targets: dict[Path, Path] = {}
    if convert_heic:
        for actual_source, virtual_source in sorted(
            virtual_paths.items(), key=lambda item: str(item[1])
        ):
            if actual_source in blocked_sources:
                continue
            if virtual_source.suffix.lower() not in HEIF_EXTENSIONS:
                continue
            if (
                image_identity(virtual_source, require_canonical=normalize_names)
                is None
            ):
                continue
            if not _valid_heif(actual_source):
                identity = image_identity(virtual_source)
                if identity is not None:
                    blocked_book_ids.add(identity[0])
                _add_issue(
                    issues,
                    kind="invalid_heic",
                    file=_relative(actual_source, base),
                    message="El archivo HEIC/HEIF no se puede abrir; no se modificará.",
                )
                continue

            jpeg_target = virtual_source.with_suffix(".jpg")
            jpeg_actual = virtual_to_source.get(jpeg_target, jpeg_target)
            legacy_target = virtual_source.with_suffix(".jpeg")
            legacy_actual = virtual_to_source.get(legacy_target, legacy_target)
            target_exists = jpeg_target in virtual_to_source or jpeg_target.exists()
            legacy_exists = legacy_target in virtual_to_source or legacy_target.exists()

            if target_exists:
                if not _valid_jpeg(jpeg_actual):
                    identity = image_identity(virtual_source)
                    if identity is not None:
                        blocked_book_ids.add(identity[0])
                    _add_issue(
                        issues,
                        kind="conversion_conflict",
                        file=_relative(actual_source, base),
                        message=(
                            "El destino existe pero no es un JPEG válido: "
                            f"{_relative(jpeg_target, base)}."
                        ),
                    )
                    continue
                conversion_target = jpeg_target
                conversion_needed = False
            elif legacy_exists and _valid_jpeg(legacy_actual):
                conversion_target = legacy_target
                conversion_needed = False
            else:
                conversion_target = jpeg_target
                conversion_needed = True

            converted_targets[virtual_source] = conversion_target
            if conversion_needed:
                actions.append(
                    {
                        "type": "convert_heic",
                        "source": _relative(virtual_source, base),
                        "destination": _relative(conversion_target, base),
                    }
                )
            if delete_original_heic:
                actions.append(
                    {
                        "type": "delete_original",
                        "source": _relative(virtual_source, base),
                        "destination": _relative(conversion_target, base),
                    }
                )

    effective_extensions = set(extensions)
    if convert_heic:
        effective_extensions -= HEIF_EXTENSIONS
        effective_extensions.update({".jpg", ".jpeg"})

    ready_paths: list[Path] = []
    ready_images: list[dict[str, Any]] = []
    seen_ready_paths: set[Path] = set()
    for actual_source, virtual_source in virtual_paths.items():
        if actual_source in blocked_sources:
            continue
        if virtual_source.suffix.lower() in HEIF_EXTENSIONS and convert_heic:
            ready_path = converted_targets.get(virtual_source)
            if ready_path is None:
                continue
        else:
            ready_path = virtual_source
        if ready_path.suffix.lower() not in effective_extensions:
            continue
        if ready_path in seen_ready_paths:
            continue
        seen_ready_paths.add(ready_path)
        identity = image_identity(ready_path, require_canonical=normalize_names)
        if identity is None:
            continue
        book_id, position, _canonical_stem = identity
        ready_paths.append(ready_path)
        ready_images.append(
            {
                "path": _relative(ready_path, base),
                "book_id": book_id,
                "position": position,
            }
        )

    warnings = sequence_warnings(ready_paths, base=base)
    blocked_book_ids.update(str(warning["book_id"]) for warning in warnings)
    for warning in warnings:
        details: list[str] = []
        if warning["missing_positions"]:
            details.append(
                "faltan posiciones "
                + ", ".join(str(item) for item in warning["missing_positions"])
            )
        if warning["duplicate_positions"]:
            details.append(
                "hay posiciones duplicadas "
                + ", ".join(str(item) for item in warning["duplicate_positions"])
            )
        _add_issue(
            issues,
            kind="sequence",
            file=str(Path(warning["folder"]) / warning["book_id"]),
            message="Secuencia de imágenes incompleta: " + "; ".join(details) + ".",
        )
    ready_images = [
        image for image in ready_images if image["book_id"] not in blocked_book_ids
    ]

    counts = Counter(action["type"] for action in actions)
    return {
        "folder": str(base),
        "fingerprint": _fingerprint(
            base,
            files,
            normalize_names=normalize_names,
            convert_heic=convert_heic,
            delete_original_heic=delete_original_heic,
            recursive=recursive,
            extensions=extensions,
        ),
        "actions": actions,
        "issues": issues,
        "blocked_book_ids": sorted(blocked_book_ids),
        "ready_images": ready_images,
        "summary": {
            "files_inspected": len(files),
            "files_ready": len(ready_images),
            "books_ready": len({item["book_id"] for item in ready_images}),
            "renames": counts["rename"],
            "conversions": counts["convert_heic"],
            "deletions": counts["delete_original"],
            "manual_corrections": len(issues),
        },
    }


def _jpeg_image(image: Image.Image) -> Image.Image:
    oriented = ImageOps.exif_transpose(image)
    if "A" in oriented.getbands():
        rgba = oriented.convert("RGBA")
        background = Image.new("RGB", rgba.size, "white")
        background.paste(rgba, mask=rgba.getchannel("A"))
        return background
    return oriented.convert("RGB")


def _convert_heif(source: Path, target: Path) -> None:
    pillow_heif.register_heif_opener()
    temp_path: Path | None = None
    try:
        with Image.open(source) as image:
            output = _jpeg_image(image)
            icc_profile = image.info.get("icc_profile")

        temp_file = tempfile.NamedTemporaryFile(
            prefix=f".{source.stem}_",
            suffix=".jpg",
            dir=source.parent,
            delete=False,
        )
        temp_path = Path(temp_file.name)
        temp_file.close()
        options: dict[str, Any] = {"format": "JPEG", "quality": 95, "optimize": True}
        if icc_profile:
            options["icc_profile"] = icc_profile
        output.save(temp_path, **options)
        if not _valid_jpeg(temp_path):
            raise ValueError("El JPEG generado no superó la validación.")
        if target.exists():
            raise ValueError(
                f"El destino apareció durante la conversión: {target.name}"
            )
        os.replace(temp_path, target)
        temp_path = None
    finally:
        if temp_path is not None:
            temp_path.unlink(missing_ok=True)


def apply_preparation_plan(plan: dict[str, Any]) -> dict[str, Any]:
    """Apply a freshly rebuilt server-side plan in its declared order."""
    base = Path(str(plan["folder"])).resolve()
    applied = Counter()
    failures: list[dict[str, str]] = []

    for action in plan["actions"]:
        if action["type"] != "rename":
            continue
        source = base / action["source"]
        target = base / action["destination"]
        try:
            if not source.is_file():
                raise ValueError("El archivo de origen ya no existe.")
            case_only_rename = _is_case_only_rename(source, target)
            if target.exists() and not case_only_rename:
                raise ValueError("El archivo de destino ya existe.")
            if case_only_rename:
                temp_file = tempfile.NamedTemporaryFile(
                    prefix=f".{source.stem}_rename_",
                    suffix=source.suffix,
                    dir=source.parent,
                    delete=False,
                )
                temp_path = Path(temp_file.name)
                temp_file.close()
                temp_path.unlink()
                source.rename(temp_path)
                try:
                    temp_path.rename(target)
                except OSError:
                    temp_path.rename(source)
                    raise
            else:
                source.rename(target)
            applied["rename"] += 1
        except (OSError, ValueError) as exc:
            failures.append({"file": action["source"], "error": str(exc)})

    for action in plan["actions"]:
        if action["type"] != "convert_heic":
            continue
        source = base / action["source"]
        target = base / action["destination"]
        try:
            if not source.is_file():
                raise ValueError("El HEIC/HEIF de origen ya no existe.")
            _convert_heif(source, target)
            applied["convert_heic"] += 1
        except (OSError, ValueError) as exc:
            failures.append({"file": action["source"], "error": str(exc)})

    for action in plan["actions"]:
        if action["type"] != "delete_original":
            continue
        source = base / action["source"]
        target = base / action["destination"]
        try:
            if not _valid_jpeg(target):
                raise ValueError(
                    "No hay un JPEG válido que permita borrar el original."
                )
            source.unlink()
            applied["delete_original"] += 1
        except (OSError, ValueError) as exc:
            failures.append({"file": action["source"], "error": str(exc)})

    failed_book_ids = {
        identity[0]
        for failure in failures
        if (identity := image_identity(failure["file"])) is not None
    }
    return {
        "renamed": applied["rename"],
        "heic_converted": applied["convert_heic"],
        "heic_originals_deleted": applied["delete_original"],
        "failed": len(failures),
        "failed_book_ids": sorted(failed_book_ids),
        "failure_examples": failures[:30],
    }


def sequence_warnings(
    paths: list[Path], *, base: Path, require_canonical: bool = False
) -> list[dict[str, Any]]:
    groups: dict[tuple[Path, str], list[int]] = {}
    for path in paths:
        identity = image_identity(path, require_canonical=require_canonical)
        if identity is None:
            continue
        book_id, position, _canonical_stem = identity
        groups.setdefault((path.parent, book_id), []).append(position)

    warnings: list[dict[str, Any]] = []
    for (parent, book_id), positions in sorted(
        groups.items(), key=lambda item: str(item[0])
    ):
        counts = Counter(positions)
        duplicates = sorted(position for position, count in counts.items() if count > 1)
        maximum = max(positions)
        missing = sorted(set(range(1, maximum + 1)) - set(positions))
        if not duplicates and not missing:
            continue
        warnings.append(
            {
                "book_id": book_id,
                "folder": _relative(parent, base),
                "missing_positions": missing,
                "duplicate_positions": duplicates,
            }
        )
    return warnings
