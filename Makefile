DEFAULT_BACKUP := data/raw/dataset_raw.sql

# Dataset collection
get_dataset:
	python src/data/main.py --token $(or $(token),$(GITHUB_TOKEN)) \
	               --repos $(or $(repos),50) \
	               --workers $(or $(workers),10) \
	               --database-url $(or $(database_url),postgresql://postgres:password@localhost/github_analysis)

restore:
	bash scripts/restore_backup.sh $(or $(file),$(DEFAULT_BACKUP))

analyze:
	python src/analysis.py --database-url $(or $(database_url),postgresql://postgres:password@localhost/github_analysis) \
	                       --clusters $(or $(clusters),3) \
	                       --workers $(or $(workers),) \
	                       --chunk-size $(or $(chunk_size),1000)

help:
	@echo "Available commands:"
	@echo "  make get_dataset    - Collect GitHub dataset"
	@echo "  make analyze        - Run analysis"
	@echo "  make restore        - Restore database from backup"
	@echo ""
	@echo "Examples:"
	@echo "  make get_dataset token=ghp_xxx repos=100"
	@echo "  make analyze clusters=5 workers=4"
	@echo "  make restore file=backups/custom.sql"