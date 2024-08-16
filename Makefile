.DEFAULT_GOAL := run

.PHONY: test
test:
	python -m unittest tests/test_*.py

.PHONY: run
run: test
	python bot.py

.PHONY: dev
dev:
	python bot.py

.PHONY: clean
clean:
	rm -rf __pycache__ **/__pycache__
