---
nav_order: 4
---

# Directory Structure

The `datayoga init` command produces the following directory structure:

```bash
.
├── .gitignore
├── data
│   └── sample.csv
├── connections.dy.yaml
└── jobs
    └── sample
        └── hello.dy.yaml
```

- `.gitignore`: For convenience, this is used to ignore the data folder.
- `data`: Folder to store data input files or output. This folder can be located anywhere as long as the runner has access to it.
- `connections.dy.yaml`: Contains definitions of source and target connectors and other general settings.
- `jobs`: Source job YAMLs. These can be nested and referenced as modules using a dot notation. e.g. `jobs/sample/hello.dy.yaml` is referenced as `sample.hello` when running the job.
