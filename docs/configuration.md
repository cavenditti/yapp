# Configuration

Configuration is done using a YAML file, `pipelines.yml`.
Yapp automatically searches for it inside current working directory or in the specified path

Top level keys are names of pipelines or `+all`, used to define a global configuration.

!!! warning

	`pipelines.yml` specification can change anytime for `0.x.x` versions.

!!! note

	I wanted to write down a proper YAML schema at first but ended up with this quick notation,
	mainly 'cause I'm lazy.

	Every tag enclosed in "< >" is a generic placeholder, which type is later specified.

``` yaml title="version 0.1.x"
$pipeline:
	inputs: # optional
		- from: <adapter>
		  with: <params> # optional
		  expose: # optional
			- use: <source>
			  as: <input(s) name(s)>
	outputs: # optional
		- to: <adapter>
		  with: <params> # optional
	hooks: # optional
		- run: <hook>
		  on: pipeline_start
		- run: <hook>
		  on: job_start
		- run: <hook>
		  on: job_finish
		- run: <hook>
		  on: pipeline_finish

	steps: # required
		- run: <step>
		  after: <step>
		  with: <params>
```

* `<adapter>` : `str` referring to the InputAdapter class
* `<params>`: `dict` with the args to be passed as arguments to \_\_init\_\_
* `<hook>` : `str` referring to the hook function
* `<source>` : `str` containing the key to pass to the `get` method
* `<step>` : `str` referring to the Job class or function for the job

`str` used as `<adapter>`, `<hook>` and `<step>` should be valid Python module strings.



## Pipeline fields

A pipeline **can** specify the following attributes, the only strictly required is `steps`.

### **`steps`**
Used to define the jobs that make up the pipeline and their dependencies.

Contains a list, each element represents a Job and its dependencies.

**For each element the following fields are valid:**

- `run`
- `after`
- `with`

### **`inputs`**
Used to define input sources.

Contains a list, each element represents an InputAdapter and its required arguments.
Also Contains a list of inputs to be exposed from that InputAdapter object defined.

**For each element the following fields are valid:**

- `from`
- `with`
- `expose`
  - `use`
  - `as`

### **`outputs`**
Used to define outputs to write results to.

Contains a list, each element represents an OutputAdapter and its required arguments.

**For each element the following fields are valid:**

- `to`
- `with`

### **`hooks`**
Used to define the hooks to perform at specific events.

Contains a list, each element represents a hook.

**For each element the following fields are valid:**

- `run`
- `on`


## Special tags
Tags defined by yapp that can be used in `pipelines.yml`, these only alter the structure of the
configuration file: they are parsed first, then the resulting configuration is parsed as usual.

### `!env`
Reads an environment variable.

If there is a `DATA_DIR` environment variable, to which is assigned the value `../data/latest`, the
following:

`!env DATA_DIR`

is automatically replaced with `../data/latest`.

### `!pipe`
Can only be used as a `step`.

Convinience shortcut for passing the first output of the last step to a function and return the
result.

It uses yapp.cli.extra_tags.pipe.Pipe, which takes the function and it's kwargs

It converts this:

``` yaml
- !pipe:
  run: prova					# name of the function
  module: test					# where to search the function
  after: common.PostProcessor
```

To something like this:

``` yaml
- run: yapp.cli.extra_tags.pipe.Pipe,
  name: _prova_1, # unique name for the step
  with: {
    fn: prova,
    base_paths: [
	  # a list of search path, for internal use
    ],
    module: test
  },
  after: common.PostProcessor
```

