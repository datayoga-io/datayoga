# Introduction

DataYoga is a framwork for building and generating data pipelines. The DataYoga CLI helps define data pipelines using a semantic markup language using yaml files. These pipeline definitions are then used to generate executable artifacts running on a variety of Processing engines such as PySpark.

![DataYoga concept](https://github.com/datayoga-io/datayoga/blob/main/docs/datayoga_concept.png?raw=true)

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

Read the [guide](https://datayoga.io/docs/guide/) for a more detailed tutorial or check out the [reference](https://datayoga.io/docs/reference/CLI.html) to see various blocks types currently available.

## Using the Spark runner

In order to run a Job as an ETL (Extract-Transform-Load) job, DataYoga also supports Spark as a `runner`.

### Install local Spark runner

To run jobs locally, datayoga uses data processing technologies called `runners`. We provide a packaged docker container with a pre-installed Spark runner.

```bash
docker run -it --name dy-spark-runner --add-host host.docker.internal:host-gateway -p 8998:8998 -p 8000:8000 -v $(pwd)/data:/opt/dy/data datayoga/dy-runner-spark:latest
```

::: warning Note
We are mapping the volume of `/opt/dy/data` to the folder named `data`. If you are running this from the datayoga project home folder, this should have been created as part of the `init` command. You can point this volume to any local folder that holds the input files for the jobs.

:::

### Updating the job to run on pyspark

To update our code to run on `pyspark` instead of `nodejs`, we will update the source yaml of our job.

Open `src/pipelines/customer/sample.yaml`:

On line 4, notice that `runs_on` determines the runner to use for the job:

```yaml
jobs:
  customer-sample:
    description: this is a basic job to load customer data from CSV file into a table
    runs_on: nodejs
```

Change the `runs_on` key to indicate `pyspark`. Modified 4 lines should appear as:

```yaml
jobs:
  customer-sample:
    description: this is a basic job to load customer data from CSV file into a table
    runs_on: pyspark
```

### Validating the install

Let's re-run the sample job:

```
dy-cli run sample.customer
```

If all goes well, you should see some startup logs, and eventually:

```
+-----+-----+
|   id| name|
+-----+-----+
|hello|world|
+-----+-----+
```

That's it! You've created your first job that loads data from CSV, runs it through Spark, and shows the data to the standard output.

Read on for a more detailed tutorial or check out the reference to see the different block types currently available.
