import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.backend.services import migrations  # noqa: E402


def main() -> None:
    status = migrations.migrate()
    print(
        "Migrations ready: "
        f"{status['applied_count']} applied, "
        f"{status['pending_count']} pending, "
        f"applied now: {status.get('applied_now', [])}"
    )


if __name__ == "__main__":
    main()
