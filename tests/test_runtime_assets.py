from scripts.prepare_local_assets import ISO_HEADER, is_iso_table, is_png


def test_iso_table_validation(tmp_path):
    iso_table = tmp_path / "iso-639-3.tab"
    iso_table.write_text(f"{ISO_HEADER}\naaa\t\t\t\tI\tL\tGhotuo\t\n", encoding="utf-8")

    assert is_iso_table(iso_table)


def test_png_validation(tmp_path):
    app_icon = tmp_path / "dani.png"
    app_icon.write_bytes(b"\x89PNG\r\n\x1a\nlocal-icon")

    assert is_png(app_icon)
