# Install the project and build its dependencies.
install:
    python -m venv venv
    venv/bin/pip install -e '.[dev]'

# build sdist and wheel into dist/
build:
    venv/bin/pip install build
    venv/bin/python -m build

# lint project
lint:
    venv/bin/flake8 pystalk/ tests/

# run test suite
test:
    venv/bin/pytest --cov=pystalk/ --cov-report=term-missing --cov-fail-under=60 tests/

# type check mypy
mypy:
    venv/bin/mypy pystalk/ tests/