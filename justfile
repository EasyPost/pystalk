# Install the project and build its dependencies.
install:
    python -m venv venv
    venv/bin/pip install -e '.[dev]'

# build sdist and wheel into dist/
build:
    venv/bin/pip install build
    venv/bin/python -m build
