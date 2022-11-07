# DataYoga Project

## Directory Structure

```bash
.
├── .gitignore
├── data
│   └── sample.csv
├── connections.yaml
└── jobs
    └── sample
        └── hello.yaml
```

- `.gitignore`: For convenience, this is used to ignore the data folder.
- `data`: Folder to store data input files or output. This folder can be located anywhere as long as the runner has access to it.
- `connections.yaml`: Contains definitions of source and target connectors and other general settings.
- `jobs`: Source job YAMLs. These can be nested and referenced as modules using a dot notation. e.g. `jobs/sample/hello.yaml` is referenced as `sample.hello` when running the job.

## Run a Job

In order to run a job, execute this from the root directory of the project:

```bash
datayoga run sample.hello
```

Or specify the root directory of the project explicitly:

```bash
datayoga run sample.hello --dir /path/to/datayoga-dir
```
