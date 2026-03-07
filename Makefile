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
UVICORN := $(PYTHON) -m uvicorn
STREAMLIT := $(PYTHON) -m streamlit
RUFF := $(PYTHON) -m ruff
BLACK := $(PYTHON) -m black
RM_VENV := powershell -NoProfile -Command "if (Test-Path '$(VENV)') { Remove-Item -Recurse -Force '$(VENV)' }"
STOP_PORT = powershell -NoProfile -Command '$$pids = Get-NetTCPConnection -LocalPort $(1) -State Listen -ErrorAction SilentlyContinue | Select-Object -ExpandProperty OwningProcess -Unique; if ($$pids) { $$pids | ForEach-Object { Stop-Process -Id $$PSItem -Force -ErrorAction SilentlyContinue } }; exit 0'
STOP_BACK = powershell -NoProfile -Command '$$cmd = Get-CimInstance Win32_Process -ErrorAction SilentlyContinue | Where-Object { ($$_.Name -match "python|uvicorn") -and ($$_.CommandLine -match "src\\.backend\\.main:app") } | Select-Object -ExpandProperty ProcessId -Unique; if ($$cmd) { $$cmd | ForEach-Object { cmd /c "taskkill /PID $$_ /T /F >NUL 2>&1" } }; $$listen = Get-NetTCPConnection -LocalPort $(BACK_PORT) -State Listen -ErrorAction SilentlyContinue | Select-Object -ExpandProperty OwningProcess -Unique; if ($$listen) { $$listen | ForEach-Object { cmd /c "taskkill /PID $$_ /T /F >NUL 2>&1" } }; exit 0'
STOP_FRONT = powershell -NoProfile -Command '$$cmd = Get-CimInstance Win32_Process -ErrorAction SilentlyContinue | Where-Object { ($$_.Name -match "python|streamlit") -and ($$_.CommandLine -match "src/frontend/app.py") } | Select-Object -ExpandProperty ProcessId -Unique; if ($$cmd) { $$cmd | ForEach-Object { cmd /c "taskkill /PID $$_ /T /F >NUL 2>&1" } }; $$listen = Get-NetTCPConnection -LocalPort $(FRONT_PORT) -State Listen -ErrorAction SilentlyContinue | Select-Object -ExpandProperty OwningProcess -Unique; if ($$listen) { $$listen | ForEach-Object { cmd /c "taskkill /PID $$_ /T /F >NUL 2>&1" } }; exit 0'
else
PYTHON_BOOTSTRAP := python3
VENV_BIN := $(VENV)/bin
PYTHON := $(VENV_BIN)/python
PIP := $(VENV_BIN)/pip
UVICORN := $(PYTHON) -m uvicorn
STREAMLIT := $(PYTHON) -m streamlit
RUFF := $(PYTHON) -m ruff
BLACK := $(PYTHON) -m black
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

# =========================
# PORTS
# =========================
BACK_PORT ?= 8000
FRONT_PORT ?= 8501

# =========================
# PHONY
# =========================
.PHONY: setup install init-db dev-back dev-front dev stop stop-back stop-front restart clean lint format
.PHONY: ensure-env

setup:
	$(PYTHON_BOOTSTRAP) -m venv $(VENV)
	$(PYTHON) -m pip install --upgrade pip
	$(PYTHON) -m pip install -e .

install:
	$(PYTHON) -m pip install -e .

ensure-env:
	@$(PYTHON_BOOTSTRAP) -c "import pathlib, sys; sys.exit(0 if pathlib.Path(r'$(PYTHON)').exists() else 1)" || $(MAKE) setup
	@$(PYTHON) -c "import importlib.util, sys; mods=('uvicorn','streamlit','fastapi'); sys.exit(0 if all(importlib.util.find_spec(m) for m in mods) else 1)" || $(MAKE) install

init-db: ensure-env
	$(PYTHON) $(INIT_DB_SCRIPT)

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
	$(STREAMLIT) run $(FRONTEND_APP) --server.port $(FRONT_PORT)

dev: ensure-env init-db
	$(MAKE) -j 2 SKIP_ENSURE=1 dev-back dev-front

stop-back:
	@echo "Stopping backend (port $(BACK_PORT))"
	@$(STOP_BACK)

stop-front:
	@echo "Stopping frontend (port $(FRONT_PORT))"
	@$(STOP_FRONT)

stop: stop-back stop-front

restart:
	$(MAKE) stop
	$(MAKE) dev

clean:
	$(RM_VENV)

lint:
	$(RUFF) check src

format:
	$(BLACK) src
