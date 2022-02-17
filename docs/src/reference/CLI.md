# dy-cli

## Install

```sh
$ npm i dy-cli -g
```

## Usage

```sh
$ dy-cli --help
```

Help output:

```
  ____        _      __   __                   ____ _     ___
 |  _ \  __ _| |_ __ \ \ / /__   __ _  __ _   / ___| |   |_ _|
 | | | |/ _` | __/ _` \ V / _ \ / _` |/ _` | | |   | |    | |
 | |_| | (_| | || (_| || | (_) | (_| | (_| | | |___| |___ | |
 |____/ \__,_|\__\__,_||_|\___/ \__, |\__,_|  \____|_____|___|
                                |___/

Commands:
  dy-cli init <project>    Scaffold a new folder with all the subdirectories
  dy-cli exec <pipeline>   Deploy and execute a pipeline from dist folder
                           against a running datayoga_spark_runner container
                           (dy-runner-spark)
  dy-cli run <pipeline>    Build and execute the pipeline against a running
                           datayoga_spark_runner container (dy-runner-spark)
  dy-cli validate          Run against a local livy and validate the code is
                           correct
  dy-cli build [pipeline]  Builds a yaml and all its dependencies into a dist
                           folder or tar file

Options:
  --help     Show help                                                 [boolean]
  --version  Show version number                                       [boolean]
```

## Available commands

* [init](#init)
* [exec](#exec)
* [run](#run)
* [validate](#validate)
* [build](#build)

### init

```sh
$ dy-cli init --help
```

Help output:

```
dy-cli init <project>

Scaffold a new folder with all the subdirectories

Options:
  --help     Show help                                                 [boolean]
  --version  Show version number                                       [boolean]
```

### exec

```sh
$ dy-cli exec --help
```

Help output:

```
dy-cli exec <pipeline>

Deploy and execute a pipeline from dist folder against a running
datayoga_spark_runner container (dy-runner-spark)

Positionals:
  pipeline  Pipeline id to run                               [string] [required]

Options:
  --help      Show help                                                [boolean]
  --version   Show version number                                      [boolean]
  --host      Host                               [string] [default: "localhost"]
  --port      Port                                      [number] [default: 8998]
  --runner    Runner to use                             [string] [default: "js"]
  --loglevel  Logging level
      [string] [choices: "INFO", "ERROR", "DEBUG", "TRACE", "WARNING"] [default:
                                                                         "INFO"]
```

### run

```sh
$ dy-cli run --help
```

Help output:

```
dy-cli run <pipeline>

Build and execute the pipeline against a running datayoga_spark_runner container
(dy-runner-spark)

Positionals:
  pipeline  pipeline id to run                               [string] [required]

Options:
  --help      Show help                                                [boolean]
  --version   Show version number                                      [boolean]
  --host      Host                               [string] [default: "localhost"]
  --port      Port                                      [number] [default: 8998]
  --local     whether to run locally or remote         [boolean] [default: true]
  --loglevel  Logging level
      [string] [choices: "INFO", "ERROR", "DEBUG", "TRACE", "WARNING"] [default:
                                                                         "INFO"]
```

### validate

```sh
$ dy-cli validate --help
```

Help output:

```
dy-cli validate

Run against a local livy and validate the code is correct

Options:
  --help     Show help                                                 [boolean]
  --version  Show version number                                       [boolean]
  --host     Host                                [string] [default: "localhost"]
  --port     Port                                       [number] [default: 8998]
```

### build

```sh
$ dy-cli build --help
```

Help output:

```
dy-cli build [pipeline]

Builds a yaml and all its dependencies into a dist folder or tar file

Options:
  --help     Show help                                                 [boolean]
  --version  Show version number                                       [boolean]
  --clean    clear the dist folder                    [boolean] [default: false]
  --runner   runner to use as a runtime target. e.g. pyspark
                                                   [string] [default: "pyspark"]
  --console  print generated code to console          [boolean] [default: false]
```