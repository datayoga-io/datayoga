---
title: Getting started
layout: page
nav_order: 1
---

# Getting started

## Download the CLI

UBUNTU20.04

```bash
wget https://datayoga.io/releases/dy_cli/latest/dy_cli-ubuntu20.04-latest.tar.gz -O /tmp/dy_cli.tar.gz
```

UBUNTU18.04

```bash
wget https://datayoga.io/releases/dy_cli/latest/dy_cli-ubuntu18.04-latest.tar.gz -O /tmp/dy_cli.tar.gz
```

RHEL8

```bash
wget https://datayoga.io/releases/dy_cli/latest/dy_cli-rhel8-latest.tar.gz -O /tmp/dy_cli.tar.gz
```

RHEL7

```bash
wget https://datayoga.io/releases/dy_cli/latest/dy_cli-rhel7-latest.tar.gz -O /tmp/dy_cli.tar.gz
```

For other platforms, see installing from pip

## Install the CLI

Unpack the downloaded file into /usr/local/bin/ directory:

```bash
sudo tar xvf /tmp/dy_cli.tar.gz -C /usr/local/bin/
```

Verify that the installation completed successfully by running the following command:

```bash
datayoga --version
```

\*Note: Non-root users should unpack to a directory with write permission and run `datayoga` directly from it.

## Create a new datayoga project

To create a new datayoga project, use the init command.

```bash
datayoga init hello_world
cd hello_world
```

## Validating the install

Let's run our first job. It is pre-defined in the samples folder as part of the init command.

```bash
datayoga run sample.hello
```

If all goes well, you should see some startup logs, and eventually:

```bash
+-----+-----+
| id  | name|
+-----+-----+
|hello|world|
+-----+-----+
```

That's it! You've created your first job that loads data from CSV, runs it through a series of transformation steps, and shows the data to the standard output. A good start. Read on for a more detailed tutorial or check out the reference to see the different block types currently available.
