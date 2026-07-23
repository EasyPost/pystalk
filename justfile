# Install the project and build its dependencies.
install:
    python -m pip install --upgrade pip
    python -m pip install -e . -r requirements-tests.txt

# build sdist and wheel into dist/
build:
    python -m pip install build
    python -m build
