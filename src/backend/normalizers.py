import re
from pathlib import Path

BOOK_ID_PATTERN = re.compile(r"^(\d{1,2})([A-Ca-c])(\d{1,4})$")
BOOK_ID_EXTRACT_PATTERN = re.compile(r"(\d{1,2})([A-Ca-c])(\d{1,4})")


def normalize_book_id(raw: str | None) -> str | None:
    text = str(raw or "").strip()
    if not text:
        return None
    match = BOOK_ID_PATTERN.match(text)
    if not match:
        return None
    module, block, number = match.groups()
    return f"{module.zfill(2)}{block.upper()}{number.zfill(4)}"


def extract_book_id_from_path(path: str | Path) -> str | None:
    stem = Path(path).stem
    match = BOOK_ID_EXTRACT_PATTERN.search(stem)
    if not match:
        return None
    module, block, number = match.groups()
    return f"{module.zfill(2)}{block.upper()}{number.zfill(4)}"


def split_book_id(book_id: str) -> tuple[str, str, str] | None:
    normalized = normalize_book_id(book_id)
    if not normalized:
        return None
    return normalized[:2], normalized[2], normalized[3:]


def clean_isbn(raw: str | None) -> str:
    text = str(raw or "").strip().replace("-", "").replace(" ", "")
    return text.upper()


def is_valid_isbn(raw: str | None) -> bool:
    isbn = clean_isbn(raw)
    if re.fullmatch(r"\d{9}[0-9X]", isbn):
        total = 0
        for index, char in enumerate(isbn):
            value = 10 if char == "X" else int(char)
            total += (10 - index) * value
        return total % 11 == 0

    if re.fullmatch(r"\d{13}", isbn):
        total = 0
        for index, char in enumerate(isbn):
            factor = 1 if index % 2 == 0 else 3
            total += int(char) * factor
        return total % 10 == 0

    return False


def extract_valid_isbn(text: str | None) -> str | None:
    body = str(text or "")
    for candidate in re.findall(r"[\dXx\- ]{9,}", body):
        normalized = clean_isbn(candidate)
        if is_valid_isbn(normalized):
            return normalized
    return None
