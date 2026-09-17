import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.backend.config import DB_PATH  # noqa: E402
from src.backend.services import migrations  # noqa: E402


def main() -> None:
    status = migrations.migrate()
    print(f"Database ready: {DB_PATH}")
    print(f"Schema version: {status['schema_version']}")
    print(f"Migrations applied now: {status.get('applied_now', [])}")


if __name__ == "__main__":
    main()
