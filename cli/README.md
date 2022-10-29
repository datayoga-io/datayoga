# DataYoga CLI

## Development

To set up environment in development mode:

### Set up venv

```
python -m venv venv
source venv/bin/activate
```

### Upgrade pip to latest version

Pip version 22 and up is needed for editable install

```
python -m pip install --upgrade pip
```

### Install dependencies

```
cd datayoga-py
python -m pip install -e .
```

## Run CLI in development mode

```
python ./cli/src/datayoga_cli/__main__.py
```
