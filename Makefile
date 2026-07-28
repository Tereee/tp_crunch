# crunch — raccourcis. `make help` pour la liste.
MONTH ?= 2026-06

.PHONY: help data verify up1 up2 up4 down check top100 circadian campaign all analyse clean-out

help:
	@grep -E '^[a-zA-Z0-9_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
	  | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

data:       ## télécharge les 168 fichiers du mois MONTH
	./scripts/download_data.sh $(MONTH)

verify:     ## vérifie l'intégrité des .gz
	./scripts/download_data.sh --verify $(MONTH)

up1:        ## démarre la variante 1 worker
	./run_job.py up 1w
up2:        ## démarre la variante 2 workers
	./run_job.py up 2w
up4:        ## démarre la variante 4 workers
	./run_job.py up 4w

down:       ## arrête la variante courante
	./run_job.py down

check:      ## état de la stack
	./run_job.py check

top100:     ## un run top100
	./run_job.py top100 $(MONTH)

circadian:  ## un run circadian
	./run_job.py circadian $(MONTH)

campaign:   ## les 4 runs imposés de la variante V (make campaign V=2w)
	./run_job.py campaign $(V) $(MONTH)

all:        ## la campagne complète : 12 runs, les 3 variantes
	./run_job.py up 1w && ./run_job.py campaign 1w $(MONTH) && ./run_job.py down 1w
	./run_job.py up 2w && ./run_job.py campaign 2w $(MONTH) && ./run_job.py down 2w
	./run_job.py up 4w && ./run_job.py campaign 4w $(MONTH) && ./run_job.py down 4w
	@$(MAKE) analyse

analyse:    ## tableaux + figure pour le rapport
	python3 tools/analyse_runs.py --plot

clean-out:  ## supprime les sorties horodatées (garde runs.csv)
	find data/out -maxdepth 1 -mindepth 1 -type d -exec rm -rf {} +
