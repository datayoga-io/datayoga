---
nav_order: 5
---

# Directory Structure

The `datayoga init` command produces the following directory structure:

```
.
├── .dyrc
├── .gitignore
├── data
│   └── sample.csv
├── connections.yaml
└── src
    ├── catalog
    │   └── sample.yaml
    └── jobs
        └── sample
            └── hello.yaml
```

- `.dyrc`: Used to store global configuration.
- `.gitignore`: For convenience, this is used to ignore the data folder and dist folder
- `data`: Folder to store data input files or output. This folder can be located anywhere as long as the runner has access to it
- `connections.yaml`: Contains definitions of source and target connectors and other general settings
- `src`: Source job yamls and catalog.
- `src/catalog`: Yaml files defining the source and target descriptors, including flat file definitions
- `src/jobs`: Source job yamls. These can be nested and referenced as modules using a dot notation. e.g. `jobs/sample/hello.yaml` is referenced as `sample.hello` when running or building the job.
