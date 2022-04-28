# yapp - Yet Another Python (data) Pipeline

yapp is a simple python data pipeline framework, it is inspired by [ploomber](https://github.com/ploomber/ploomber).

**Development is still in early stages, many things may change in the future**

yapp strives to be as simple as possible and make you focus on the correctness of your algorithms.
It's developed with specific requirements and built according to those: it may be the best choice for you once completed, or may be not.
Basic functionality is there but there are some very rough edges to be smoothed. There are no tests not even a proper example yet.


## Install

```
pip install yapp-pipelines
```

## Usage

Pipelines are described using in a `pipelines.yml` yaml file.
This file contains the pipelines definitions and an optional global config for all the pipelines.

A Pipeline is made up of Jobs.
A Job represents a step of the pipeline, it takes inputs as parameters and returns a dict of outputs.
The pipeline.yml file defines the dependencies of every Job in the Pipeline. They are resolved and
then run one at the time (even if it may be possible to run them in parallel, this is a
willingly design choice).

Pipelines can have hooks to perform specific task before or after each task (such as updating some
kind of status monitor)

For a complete overview on how to define pipelines se the [documentation in the wiki.](https://github.com/cavenditti/yapp/wiki/pipelines.yml)

You can run a pipeline using the `yapp` command:
```
yapp [-h] [-p [PATH]] [-d] pipeline
```

yapp automatically searches for classes and functions you use in your yaml files.
It searches in, in order:

 1. The pipeline directory (if it exists)
 2. Top level directory of your code
 3. yapp built-in modules

The first two are relative to the current working directory or to the supplied using `path` or `-p`

## Planned features still missing:

 - [ ] pipelines.yml specification
 - [ ] A good and working example
 - [ ] docstrings
 - [ ] TESTS.
 - [ ] Pipeline status monitor class with an example
 - [ ] Allow importing from Jupyter notebooks
 - [ ] Inputs aliases
 - [ ] Consider permitting repeted tasks in a single pipeline (can this be useful?)
 - [ ] For each step, keep track of the inputs required in future steps. So that unneeded ones can be
   removed from memory
