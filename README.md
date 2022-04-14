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

A Pipeline is made up of Jobs.
A Job represents a step of the pipeline, it takes inputs as parameters and returns a dict of outputs.
The pipeline.yml file defines the dependencies of every Job in the Pipeline. They are resolved and
then they are run one at the time (even if it may be possible to run them in parallel, this is a
willingly design choice).

Pipelines can have hooks to perform specific task before or after each task (such as updating some
kind of status monitor)

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
 - [x] yapp cli command
 - [ ] Finalize yaml files specification
 - [ ] Proper code organization
 - [x] Package
 - [ ] A good and working example
 - [ ] hooks
 - [ ] Working global config for multiple pipelines

### Possibly lower priority
 - TESTS.
 - ~Good~ Better logging
 - docstrings
 - Add sample pipeline status monitor class
 - Consider permitting repeted tasks in a single pipeline (can this be useful?)
