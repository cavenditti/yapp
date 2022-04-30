# Configuration

Configuraiton is done using a `pipelines.yml` file.

Top level keys are names of pipelines or `+all`, used to define a global configuration.

!!! warning

	`pipelines.yml` specification can change anytime for `0.x.x` versions.

!!! note

	I wanted to write down a proper YAML schema at first but ended up with this quick notation,
	mainly 'cause I'm lazy.

	Every tag starting with "$" is a generic placeholder, which type is later specified.

``` yaml title="version 0.1.x"
$pipeline:
	inputs: # optional
		- $adapter
	outputs: # optional
		- $adapter
	expose: # optional
		- $expose_map
	hooks: # optional
		on_pipeline_start:
			- $hook
		on_job_start:
			- $hook
		on_job_finish:
			- $hook
		on_pipeline_finish:
			- $hook

	steps: # required
		- $step
```

* `$adapter` : `str` or `dict` with the first key used as name and the value ignored, the others are
  passed as arguments to \_\_init\_\_
* `$hook` : `str`
* `$expose_map` : `dict` with a single pair `str`, [list of `dict` with a single `str` key-value pair]
* `$step` : `str` or `dict` with a single `str` key-value pair, or a sigle `str`:`list` pair

`str` used as `$adapter`, `$hook` and `step` should be valid Python module strings.



## Pipeline fields

A pipeline **can** specify the following attributes, the only strictly required is `steps`.

###`steps`
Used to define the jobs that make up the pipeline and their dependencies.

Contains a list, each element represents a Job and its dependencies.

###`inputs`
Used to define input sources.

Contains a list, each element represents an InputAdapter and its required arguments.


###`outputs`
Used to define outputs to write results to.

Contains a list, each element represents an OutputAdapter and its required arguments.

###`expose`
Used to define aliases for inputs.

Contains a list, each element represents a list of inputs to be exposed from a source.

###`hooks`
Used to define the hooks to perform at specific events.

Contains a dict, with the keys being the possible events that trigger a hook. For each key a list of
functions to use as hooks can be specified. Every event key is optional.


## Special types
Types defined by yapp that can be used in `pipelines.yml`:

### `!env`
Reads an environment variable.

If there is a `DATA_DIR` environment variable, to which is assigned the value `../data/latest`, the
following:

`!env DATA_DIR`

is automatically replaced with `../data/latest`.
