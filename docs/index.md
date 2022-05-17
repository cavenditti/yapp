# **yapp** — Yet Another Python (data) Pipeline

yapp is a simple python data pipeline framework, inspired by [ploomber](https://github.com/ploomber/ploomber).

yapp allows you to quickly define pipelines to pack together your messy Python experiments and make better
use of them.

Code is hosted on [GitHub](https://github.com/cavenditti/yapp).

!!! warning

	**Development is still in early stages, many things may change in the future.**

	If you are looking for a similar tool to use right away in something resembling a production environment, just
	use Ploomber.

yapp strives to be as simple as possible and make you focus on the correctness of your algorithms.
It's developed with specific requirements in mind and built according to those: it may be the best choice for you once completed, or may be not.
Basic functionality is there but there are some very rough edges to be smoothed. There are no tests and not even a proper example yet.



## Install

```
pip install yapp-pipelines
```



## Usage

To use yapp-pipelines you need two things:

* A directory containing your code
* A `pipelines.yml` file inside it

Pipelines configuration is defined in a `pipelines.yml`, it contains all the steps to perform in each of your pipelines and all the needed resources.

A Pipeline is made up of Jobs.
A Job represents a step of the pipeline, it takes inputs as parameters and returns a dict of outputs.
A Job can a class that extends `yapp.Job`, or just a function returning a dictionary.
The configuration defines all the dependencies of every Job in the Pipeline. They are resolved and
then run one at the time (even if it may be possible to run them in parallel, this is a
willingly design choice).

Pipelines can have hooks to perform specific task before or after each task (such as updating some
kind of status monitor)

For a complete overview on how to define pipelines se the [Configuration page](configuration.md).

You can then run your pipeline with `yapp [pipeline name]`.



## The `yapp` command

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



## Example

``` title="Code Structure"
example_project/
	├──my_pipeline/				# pipeline directory
	│	├──MainTask.py			# MainTask class inside a bad CamelCase filename
	│	├──another_task.py		# AnotherTask inside a better snake_case filename
	│	└──...
	├──data/
	│	├──raw.csv				# Input data
	│	└──...
	└──pipelines.yml
```

``` yaml title="pipelines.yml"
my_pipeline:					# define a new pipeline
  inputs:
	- from: file.CsvInput		# use input from CSV files
	  wih:
		directory: "./data"
	  expose:
		- use: "raw.csv"		# Read "raw.csv"…
		  as: raw_data			# …and use it as the raw_data input

  steps:
	- run: MainTask
	- run: AnotherTask
	  after: MainTask			# AnotherTask must run only after MainTask
```
