DEFAULT_BACKUP := data/raw/dataset_raw.sql

# Dataset collection
get_dataset:
	python src/main.py --token $(or $(token),$(GITHUB_TOKEN)) \
	               --repos $(or $(repos),50) \
	               --workers $(or $(workers),10) \
	               --database-url $(or $(database_url),postgresql://postgres:password@localhost/github_analysis)

restore:
	bash scripts/restore_backup.sh $(or $(file),$(DEFAULT_BACKUP))