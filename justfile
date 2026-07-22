# Install the project and build its dependencies.
install:
    uv venv
    uv pip install --system -e . -r requirements-tests.txt

# build sdist and wheel into dist/
build:
    uv build
