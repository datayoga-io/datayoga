# DataYoga CLI

## Development

To set up environment in development mode:

### Set Up Virtual Environment

```bash
python -m venv venv
source venv/bin/activate
```

### Upgrade `pip` to Latest Version

> [Pip](https://pypi.org/project/pip) version 22 and up is needed for editable install.

```bash
python -m pip install --upgrade pip
```

### Install Dependencies

```bash
cd core
python -m pip install -e .
```

## Run CLI in Development Mode

```bash
python ./cli/src/datayoga/__main__.py
```

## Running tests

### Core

```
cd core
pip install .
pip install .[test]
PYTHONPATH=src python -m pytest -s --log-cli-level=DEBUG
```
