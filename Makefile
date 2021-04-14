clean: clean-build clean-pyc clean-test

clean-build:
	rm -fr build/
	rm -fr dist/
	rm -fr .eggs/
	find . -name '*.egg-info' -exec rm -fr {} +
	find . -name '*.egg' -exec rm -f {} +

clean-pyc:
	find . -name '*.pyc' -exec rm -f {} +
	find . -name '*.pyo' -exec rm -f {} +
	find . -name '*~' -exec rm -f {} +
	find . -name '__pycache__' -exec rm -fr {} +

clean-test:
	rm -fr .tox/
	rm -f .coverage
	rm -fr htmlcov/


test: clean-test
	pytest --cov=shark/calc --cov-report=term-missing tests/ -v -s

test-single-module: clean-test
	pytest $(module) -v -s


install-dev: clean
	pip install -r requirements_dev.txt
	pip install -e . 


install-dev-force-reinstall: clean 
	pip install -r requirements_dev.txt
	pip install -e . --force-reinstall


install: clean 
	pip install . 