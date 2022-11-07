---
nav_order: 2
---

# Getting Started

## Create Python Virtual Environment

```bash
python -m venv venv
source venv/bin/activate
```

## Install DataYoga CLI

```bash
pip install datayoga
```

Verify that the installation completed successfully by running this command:

```bash
datayoga --version
```

## Create New DataYoga Project

To create a new DataYoga project, use the `init` command:

```bash
datayoga init hello_world
cd hello_world
```

> [Directory structure](directory-structure.md)

## Run Your First Job

Let's run our first job. It is pre-defined in the samples folder as part of the `init` command:

```bash
datayoga run sample.hello
```

If all goes well, you should see some startup logs, and eventually:

```yaml
{"greeting": "Hello Mr. John Doe"}
{"greeting": "Hello Ms. Jane Doe"}
{"greeting": "Hello Mr. Bill Adams"}
```

That's it! You've created your first job that loads data from CSV, runs it through a series of transformation steps, and shows the data to the standard output. A good start. Read on for a more detailed tutorial or check out the [reference](reference/blocks.md) to see the different block types currently available.
