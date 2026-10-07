from pathlib import Path

import pillow_heif
import pytest
from PIL import Image

from src.backend.services import books
from src.backend.services.input_preparation import (
    apply_preparation_plan,
    build_preparation_plan,
)

IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".webp", ".heic", ".heif"}


def _save_image(path: Path, *, format: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if format == "HEIF":
        pillow_heif.register_heif_opener()
    Image.new("RGB", (12, 8), "red").save(path, format=format)


def test_plan_lists_all_changes_without_modifying_files(tmp_path: Path):
    module = tmp_path / "A" / "01"
    source = module / "1a1_02.HEIC"
    invalid = module / "foto_sin_identificador.heic"
    _save_image(source, format="HEIF")
    _save_image(invalid, format="HEIF")

    plan = build_preparation_plan(
        module,
        recursive=True,
        extensions=IMAGE_EXTENSIONS,
        normalize_names=True,
        convert_heic=True,
        delete_original_heic=True,
    )

    assert source.exists()
    assert invalid.exists()
    assert [action["type"] for action in plan["actions"]] == [
        "rename",
        "convert_heic",
        "delete_original",
    ]
    assert plan["summary"] == {
        "files_inspected": 2,
        "files_ready": 0,
        "books_ready": 0,
        "renames": 1,
        "conversions": 1,
        "deletions": 1,
        "manual_corrections": 2,
    }
    assert [issue["kind"] for issue in plan["issues"]] == [
        "invalid_name",
        "sequence",
    ]

    applied = apply_preparation_plan(plan)

    output = module / "01A0001_2.jpg"
    assert applied["failed"] == 0
    assert applied["renamed"] == 1
    assert applied["heic_converted"] == 1
    assert applied["heic_originals_deleted"] == 1
    assert output.is_file()
    assert not (module / "01A0001_2.heic").exists()
    assert invalid.exists()
    with Image.open(output) as image:
        assert image.format == "JPEG"
        assert image.size == (12, 8)


def test_existing_valid_jpeg_is_reused_without_false_duplicate(tmp_path: Path):
    module = tmp_path / "A" / "01"
    _save_image(module / "01A0001.heic", format="HEIF")
    _save_image(module / "01A0001.jpg", format="JPEG")

    plan = build_preparation_plan(
        module,
        recursive=True,
        extensions=IMAGE_EXTENSIONS,
        normalize_names=True,
        convert_heic=True,
        delete_original_heic=False,
        expected_block="A",
        expected_module="01",
    )

    assert plan["actions"] == []
    assert plan["issues"] == []
    assert plan["summary"]["files_ready"] == 1
    assert plan["summary"]["books_ready"] == 1


def test_plan_reports_rename_collision_for_manual_correction(tmp_path: Path):
    module = tmp_path / "A" / "01"
    _save_image(module / "1a1.jpg", format="JPEG")
    _save_image(module / "01A0001.jpg", format="JPEG")

    plan = build_preparation_plan(
        module,
        recursive=True,
        extensions=IMAGE_EXTENSIONS,
        normalize_names=True,
        convert_heic=True,
        delete_original_heic=False,
    )

    assert plan["actions"] == []
    assert plan["issues"][0]["kind"] == "rename_conflict"
    assert plan["blocked_book_ids"] == ["01A0001"]
    assert plan["summary"]["books_ready"] == 0
    assert (module / "1a1.jpg").exists()
    assert (module / "01A0001.jpg").exists()


def test_wrong_module_is_reported_without_proposing_changes(tmp_path: Path):
    module = tmp_path / "A" / "01"
    source = module / "2b3.HEIC"
    _save_image(source, format="HEIF")

    plan = build_preparation_plan(
        module,
        recursive=True,
        extensions=IMAGE_EXTENSIONS,
        normalize_names=True,
        convert_heic=True,
        delete_original_heic=True,
        expected_block="A",
        expected_module="01",
    )

    assert plan["actions"] == []
    assert plan["issues"][0]["kind"] == "scope_mismatch"
    assert plan["summary"]["files_ready"] == 0
    assert source.exists()


def test_ingest_requires_a_fresh_plan_before_file_changes(tmp_path: Path):
    for block in ("A", "B", "C"):
        (tmp_path / block).mkdir()
    module = tmp_path / "A" / "01"
    _save_image(module / "1a1.jpg", format="JPEG")

    with pytest.raises(ValueError, match="Analiza el módulo"):
        books.ingest_covers(tmp_path, block="A", module="01")

    plan = books.plan_ingest_covers(tmp_path, block="A", module="01")
    _save_image(module / "01A0002.jpg", format="JPEG")

    with pytest.raises(ValueError, match="ha cambiado desde el análisis"):
        books.ingest_covers(
            tmp_path,
            block="A",
            module="01",
            preparation_fingerprint=plan["fingerprint"],
        )
