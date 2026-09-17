# =========================
# ENV
# =========================
# Resolve everything relative to this Makefile so commands work even if
# `make -f ...` is executed from another directory.
MAKEFILE_DIR := $(abspath $(dir $(lastword $(MAKEFILE_LIST))))
-include $(MAKEFILE_DIR)/.env
export

# =========================
# PYTHON / VENV
# =========================
VENV := $(MAKEFILE_DIR)/.venv

ifeq ($(OS),Windows_NT)
PYTHON_BOOTSTRAP := py
VENV_BIN := $(VENV)/Scripts
PYTHON := $(VENV_BIN)/python.exe
PIP := $(VENV_BIN)/pip.exe
UVICORN := "$(PYTHON)" -m uvicorn
STREAMLIT := "$(PYTHON)" -m streamlit
RUFF := "$(PYTHON)" -m ruff
BLACK := "$(PYTHON)" -m black
PYTEST := "$(PYTHON)" -m pytest
RM_VENV := powershell -NoProfile -Command "if (Test-Path '$(VENV)') { Remove-Item -Recurse -Force '$(VENV)' }"
STOP_PORT = powershell -NoProfile -Command '$$pids = Get-NetTCPConnection -LocalPort $(1) -State Listen -ErrorAction SilentlyContinue | Select-Object -ExpandProperty OwningProcess -Unique; if ($$pids) { $$pids | ForEach-Object { Stop-Process -Id $$PSItem -Force -ErrorAction SilentlyContinue } }; exit 0'
STOP_BACK = powershell -NoProfile -Command '$$cmd = Get-CimInstance Win32_Process -ErrorAction SilentlyContinue | Where-Object { ($$_.Name -match "python|uvicorn") -and ($$_.CommandLine -match "src\\.backend\\.main:app") } | Select-Object -ExpandProperty ProcessId -Unique; if ($$cmd) { $$cmd | ForEach-Object { cmd /c "taskkill /PID $$_ /T /F >NUL 2>&1" } }; $$listen = Get-NetTCPConnection -LocalPort $(BACK_PORT) -State Listen -ErrorAction SilentlyContinue | Select-Object -ExpandProperty OwningProcess -Unique; if ($$listen) { $$listen | ForEach-Object { cmd /c "taskkill /PID $$_ /T /F >NUL 2>&1" } }; exit 0'
STOP_FRONT = powershell -NoProfile -Command '$$cmd = Get-CimInstance Win32_Process -ErrorAction SilentlyContinue | Where-Object { ($$_.Name -match "python|streamlit") -and ($$_.CommandLine -match "src/frontend/app.py") } | Select-Object -ExpandProperty ProcessId -Unique; if ($$cmd) { $$cmd | ForEach-Object { cmd /c "taskkill /PID $$_ /T /F >NUL 2>&1" } }; $$listen = Get-NetTCPConnection -LocalPort $(FRONT_PORT) -State Listen -ErrorAction SilentlyContinue | Select-Object -ExpandProperty OwningProcess -Unique; if ($$listen) { $$listen | ForEach-Object { cmd /c "taskkill /PID $$_ /T /F >NUL 2>&1" } }; exit 0'
else
PYTHON_BOOTSTRAP := python3
VENV_BIN := $(VENV)/bin
PYTHON := $(VENV_BIN)/python
PIP := $(VENV_BIN)/pip
UVICORN := "$(PYTHON)" -m uvicorn
STREAMLIT := "$(PYTHON)" -m streamlit
RUFF := "$(PYTHON)" -m ruff
BLACK := "$(PYTHON)" -m black
PYTEST := "$(PYTHON)" -m pytest
RM_VENV := rm -rf $(VENV)
STOP_PORT = sh -c 'pids=$$(lsof -ti :$(1) 2>/dev/null); if [ -n "$$pids" ]; then kill $$pids 2>/dev/null || true; sleep 0.7; still=$$(lsof -ti :$(1) 2>/dev/null); if [ -n "$$still" ]; then kill -9 $$still 2>/dev/null || true; fi; fi; exit 0'
# On Linux, avoid pkill patterns here because they can match the shell
# command spawned by make itself and terminate `make stop-back`.
STOP_BACK = $(call STOP_PORT,$(BACK_PORT))
STOP_FRONT = $(call STOP_PORT,$(FRONT_PORT))
endif

BACKEND_APP := src.backend.main:app
FRONTEND_APP := $(MAKEFILE_DIR)/src/frontend/app.py
INIT_DB_SCRIPT := $(MAKEFILE_DIR)/scripts/init_db.py
DB_MAINT_SCRIPT := $(MAKEFILE_DIR)/scripts/db_maintenance.py
MIGRATIONS_SCRIPT := $(MAKEFILE_DIR)/scripts/migrate_db.py
SNAPSHOTS_SCRIPT := $(MAKEFILE_DIR)/scripts/snapshots.py
PREPARE_ASSETS_SCRIPT := $(MAKEFILE_DIR)/scripts/prepare_local_assets.py
APPCTL_SCRIPT := $(MAKEFILE_DIR)/scripts/appctl.py
LOCK_SCRIPT := $(MAKEFILE_DIR)/scripts/lock_dependencies.py
GIT_REMOTE ?= origin
GIT_BRANCH ?= main
DB_PATH ?= data/books.duckdb

# =========================
# PORTS
# =========================
BACK_PORT ?= 8000
FRONT_PORT ?= 8501

# =========================
# PHONY
# =========================
.PHONY: prepare-assets setup install lock upgrade-lock check-lock build update start ensure-env init-db db-maint db-repack db-repack-replace publish-snapshot list-snapshots import-snapshot cleanup-snapshots migrate-db dev-back dev-front dev stop stop-back stop-front restart doctor smoke clean lint format test

prepare-assets:
	$(PYTHON_BOOTSTRAP) "$(PREPARE_ASSETS_SCRIPT)"

setup:
	$(PYTHON_BOOTSTRAP) "$(APPCTL_SCRIPT)" setup

install:
	$(PYTHON_BOOTSTRAP) "$(APPCTL_SCRIPT)" setup

lock:
	"$(PYTHON)" "$(LOCK_SCRIPT)"

upgrade-lock:
	"$(PYTHON)" "$(LOCK_SCRIPT)" --upgrade

check-lock:
	"$(PYTHON)" "$(LOCK_SCRIPT)" --check

build: ensure-env
	"$(PYTHON)" -m build --no-isolation

update:
	$(PYTHON_BOOTSTRAP) "$(APPCTL_SCRIPT)" update

start:
	$(PYTHON_BOOTSTRAP) "$(APPCTL_SCRIPT)" launch

ensure-env:
	$(PYTHON_BOOTSTRAP) "$(APPCTL_SCRIPT)" setup

init-db: ensure-env
	"$(PYTHON)" "$(INIT_DB_SCRIPT)"

db-maint: ensure-env
	"$(PYTHON)" "$(DB_MAINT_SCRIPT)" --db "$(DB_PATH)"

db-repack: ensure-env
	"$(PYTHON)" "$(DB_MAINT_SCRIPT)" --db "$(DB_PATH)" --repack

db-repack-replace: ensure-env
	"$(PYTHON)" "$(DB_MAINT_SCRIPT)" --db "$(DB_PATH)" --repack --replace

publish-snapshot: ensure-env
	"$(PYTHON)" "$(SNAPSHOTS_SCRIPT)" publish

list-snapshots: ensure-env
	"$(PYTHON)" "$(SNAPSHOTS_SCRIPT)" list

import-snapshot: ensure-env
	"$(PYTHON)" "$(SNAPSHOTS_SCRIPT)" import "$(SNAPSHOT_ID)" --confirm

cleanup-snapshots: ensure-env
	"$(PYTHON)" "$(SNAPSHOTS_SCRIPT)" cleanup

migrate-db: ensure-env
	"$(PYTHON)" "$(MIGRATIONS_SCRIPT)"

dev-back:
ifneq ($(SKIP_ENSURE),1)
	@$(MAKE) ensure-env
endif
	@echo "Starting backend on port $(BACK_PORT)"
	@$(MAKE) stop-back
	$(UVICORN) $(BACKEND_APP) --reload --port $(BACK_PORT)

dev-front:
ifneq ($(SKIP_ENSURE),1)
	@$(MAKE) ensure-env
endif
	@echo "Starting frontend on port $(FRONT_PORT)"
	@$(MAKE) stop-front
	$(STREAMLIT) run "$(FRONTEND_APP)" --server.port $(FRONT_PORT)

dev:
	$(PYTHON_BOOTSTRAP) "$(APPCTL_SCRIPT)" dev

stop-back:
	@echo "Stopping backend (port $(BACK_PORT))"
	@$(STOP_BACK)

stop-front:
	@echo "Stopping frontend (port $(FRONT_PORT))"
	@$(STOP_FRONT)

stop:
	$(PYTHON_BOOTSTRAP) "$(APPCTL_SCRIPT)" stop

restart:
	$(MAKE) stop
	$(MAKE) dev

doctor:
	$(PYTHON_BOOTSTRAP) "$(APPCTL_SCRIPT)" doctor

smoke: ensure-env
	"$(PYTHON)" "$(APPCTL_SCRIPT)" smoke

clean:
	$(RM_VENV)

lint: ensure-env
	$(RUFF) check src scripts tests
	$(BLACK) --check src scripts tests

format: ensure-env
	$(BLACK) src scripts tests

test: ensure-env
	$(PYTEST)
