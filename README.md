# Introduction

DataYoga is a framwork for building and generating data pipelines. The DataYoga CLI helps define data pipelines using a semantic markup language using yaml files. These pipeline definitions are then used to generate executable artifacts running on a variety of Processing engines such as PySpark.

# Data Entities

DataYoga models the data ecosystem using the following entities:

`Datastore` - A datastore represents a source or target of data that can hold data at rest or data in motion. Datastores include entities such as a table in a database, a file, or a stream. A Datastore can act either as a source or a target of a pipeline.

`File` - A file is a type of Datastore that represents information stored in files. Files contain metadata about their structure and schema.

`Dimension` - A dimension table / file is typically used for lookup and constant information that is managed as part of the application code. This often includes lookup values such as country codes.

`Runner` - A runner is an executable capable of running code. The `Runner` communicates with a `Processor` to execute the code. A `Runner` can be a local NodeJs process running queries in a database (a `Processor`), or PySpark which acts both as a `Runner` and a `Processor`.

`Processor` - A processing engine capable of running data operations. Every `Processor` supports one or more programming languages. Some `Processors`, like a database engine, may only support SQL, while others like Spark may support Python, Scala, and Java.

`Consumer` - A consumer consumes data and presents it to a user. Consumers include reports, dashboards, and interactive applications.

`Pipeline` - A pipeline represents a series of `Jobs` that operate on a single `Runner`.

`Job` - A job is composed of a series of Steps that fetch information from one or more Datastores, transform them, and store the result in a target Datastore, or perform actions such as sending out alerts or performing HTTP calls.

`Job Step` - Every step in a job performs a single action. A step can be of a certain _type_ representing the action it performs. A step can be an SQL statement, a Python statement, or a callout to a library. Steps can be chained to create a Directed Acyclic Graph (DAG).

# Getting started

## Pre-requisites

### Install NodeJS

https://nodejs.org/en/download/package-manager/

## Installing the CLI

Install the CLI

```
npm install -g @datayoga-io/datayoga
```

Verify that the installation completed successfully by running the following command:

```
dy-cli --version
```

## Create a new datayoga project

To create a new datayoga project, use the `init` command.

```
dy-cli init myproject
cd myproject
```

You will see a folder structure that scaffolds a new datayoga enviornment. The scaffold also includes a demo northwind sqlite database and sample pipelines.

## Validating the install

Let's run our first job. It is pre-defined in the samples folder as part of the `init` command.

```
dy-cli run sample.customer
```

That's it! You've created your first job that loads data from CSV, runs it through a basic transformation, and upserts the data into a target table.

Read the guide for a more detailed tutorial or check out the reference to see various blocks types currently available.
