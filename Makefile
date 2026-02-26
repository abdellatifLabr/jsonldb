MAX_LINE_LENGTH=120

.PHONY: tidy format lint start start-db stop-db

format:
	python -m black -l $(MAX_LINE_LENGTH) .
	python -m isort .

lint:
	python -m flake8 --max-line-length $(MAX_LINE_LENGTH) .

tidy: format lint
