# yapp - Yet Another Python (data) Pipeline

yapp is a simple python data pipeline framework, it is inspired by [ploomber](https://github.com/ploomber/ploomber).

**Development is still in early stages, many things may change in the future**

yapp strives to be as simple as possible and make you focus on the correctness of your algorithms.
It's developed with specific requirements and built according to those: it may be the best choice for you once completed, or may be not.
For sure it isn't right now.

## Usage

Pipelines are described using yaml files:
 - `pipelines.yml` defines the pipelines [required]
 - `config.yml`		defines a global configuration (e.g. inputs and outputs)

You can run a pipeline using:
```yapp PIPELINE_NAME [PATH]```

yapp automatically searches for classes and functions you use in your yaml files.
It searches in, in order:
	1. The pipeline directory (if it exists)
	2. Top level directory of your code
	3. yapp built-in modules

Search path order: pipeline root > root directory

## TODOs

### Basic features still missing:
 - [ ] yapp cli command
 - [ ] Finalize yaml files specification
 - [ ] Proper code organization
 - [ ] Package
 - [ ] Working examples

### Possibly lower priority
 - TESTS.
 - Good logging
 - docstrings
 - Add pipeline status monitor class
 - Consider permitting repeted tasks in a single pipeline (can this be useful?)

