from contextlib import asynccontextmanager

from fastapi import FastAPI

from src.project_meta import get_app_meta

from .config import DB_PATH
from .process_lock import DatabaseProcessLock
from .routers import books as books_router
from .routers import core as core_router
from .routers import core_books as core_books_router
from .routers import export as export_router
from .routers import ingest as ingest_router
from .routers import snapshots as snapshots_router
from .routers import workflow as workflow_router
from .services import books, migrations

APP_META = get_app_meta()

_database_process_lock = DatabaseProcessLock(DB_PATH)
_database_process_lock.acquire()


@asynccontextmanager
async def lifespan(_app: FastAPI):
    try:
        yield
    finally:
        _database_process_lock.release()


app = FastAPI(
    title=f"{APP_META.app_name} API",
    version=APP_META.version,
    lifespan=lifespan,
)


migrations.migrate()
_recovered_stale_runs = books.recover_stale_running_workflows()
if _recovered_stale_runs:
    print(f"[startup] recovered {_recovered_stale_runs} stale workflow runs")

app.include_router(core_router.router)
app.include_router(ingest_router.router)
app.include_router(workflow_router.router)
app.include_router(books_router.router)
app.include_router(core_books_router.router)
app.include_router(export_router.router)
app.include_router(snapshots_router.router)
