import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.backend.config import DB_PATH
from src.backend.services import books


def main() -> None:
    books.init_table()
    print(f"Database ready: {DB_PATH}")


if __name__ == "__main__":
    main()
