PYTHON  := .venv/bin/python
PIP     := .venv/bin/pip
STREAMLIT := .venv/bin/streamlit

.PHONY: install run pipeline dashboard stop clean

install:
	python3 -m venv .venv
	$(PIP) install --upgrade pip --quiet
	$(PIP) install -r requirements.txt --quiet
	@echo "✓ Environnement prêt"

run: ## Lancer la stack complète (MongoDB + pipeline + dashboard)
	docker compose up mongodb -d
	@sleep 5
	$(PYTHON) scripts/run_pipeline_local.py
	$(STREAMLIT) run dashboard/app.py --server.port 8501

pipeline: ## Rejouer uniquement le pipeline (sans réingérer)
	$(PYTHON) scripts/run_pipeline_local.py --skip-ingest

dashboard: ## Lancer uniquement le dashboard
	$(STREAMLIT) run dashboard/app.py --server.port 8501

stop: ## Arrêter MongoDB
	docker compose down

clean: ## Supprimer les données générées
	rm -rf data/
	@echo "✓ Données supprimées"
