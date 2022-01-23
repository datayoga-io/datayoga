# Getting started

# Pre-requisites

## Install NodeJS

https://nodejs.org/en/download/package-manager/

## Install Docker

This is needed for running a local job runner

https://docs.docker.com/get-docker/

# Installing the CLI

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

You will see a folder structure that scaffolds a new datayoga enviornment.

## Install local Spark runner

To run jobs locally, datayoga uses data processing technologies called `runners`. We provide a packaged docker container with a pre-installed Spark runner.

```
docker run -it --name dy-spark-runner --add-host host.docker.internal:host-gateway -p 8998:8998 -p 8000:8000 -v $(pwd)/data:/opt/dy/data zalmane/dy-runner-spark:latest
```

::: warning Note
We are mapping the volume of `/opt/dy/data` to the folder named `data`. If you are running this from the datayoga project home folder, this should have been created as part of the `init` command. You can point this volume to any local folder that holds the input files for the jobs.

:::

## Validating the install

Let's run our first job. It is pre-defined in the samples folder as part of the `init` command.

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

That's it! You've created your first job that loads data from CSV, runs it through Spark, and shows the data to the standard output. Not very useful, but a good start. Read on for a more detailed tutorial or check out the reference to see the different block types currently available.
