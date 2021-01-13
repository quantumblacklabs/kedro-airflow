package:
	rm -Rf dist
	python setup.py sdist bdist_wheel

install: package
	pip install -U dist/*.whl

install-pip-setuptools:
	python -m pip install -U pip setuptools wheel

lint:
	pre-commit run -a --hook-stage manual

test:
	pytest -vv tests

e2e-tests:
	behave

secret-scan:
	trufflehog --max_depth 1 .

legal:
	python tools/license_and_headers.py

clean:
	rm -rf build dist pip-wheel-metadata .pytest_cache
	find . -regex ".*/__pycache__" -exec rm -rf {} +
	find . -regex ".*\.egg-info" -exec rm -rf {} +
