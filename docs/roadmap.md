
### Ideas for a possible evolution in `1.x.x`

Current `pipelines.yml` specs are totally not good, lots of checks it conforms to the supposed
schema are done by hand because it's difficult to enforce a proper schema onto it.

A few relevant problems and ideas:

- dictionaries should not depend on key ordering
- hooks definition is bad
- Exposed variables are separated from inputs definition. This is not necessarly a bad thing but
  it probably makes less clear which inputs are used for which argument (very subjective).

A slightly better approach could be:

``` yaml title="pipelines.yml specification"
$pipeline:  # pipeline name or "+all"

	# optional
	inputs:
		- name: $input_name # name is optional
		  source: $input
		  $args
		  expose:
			  - use: $data
				as: $alias
				$args

		- $input: $input_name # input_name is optional
		  $args
		  expose:
			  - $data: $alias

	# optional
	outputs:
		- name: $output_name
		  sink: $output
		  $args

	# optional
	hooks:
		- run: $hook_func
		  on: $hook_event

	# required
	steps:
		- $step
```

or instead just do like Ploomber and define everything inside Jobs

``` yaml title="alt pipelines.yml specification"
$pipeline:  # pipeline name or "+all"
	# optional
	inputs:
		- name: $input_name # name is optional
		  source: $input
		  $args

	# optional
	outputs:
		- name: $output_name
		  sink: $output
		  $args

	# optional
	hooks:
		- run: $hook_func
		  on: $hook_event

	# required
	steps:
		- do: $step
		  inputs:
			- from: $input_name
			  use: $data
			  as: $alias
		  require: $other_steps
```

One might also thing about a kind of hybrid approach: allowing to specify aliases for inputs
inside the definition, that are automatically used inside the jobs, while also allowing to also just
define them inside steps.

Enclosing all parameters in a `with` field

``` yaml title="alt-alt pipelines.yml specification"
$pipeline:  # pipeline name or "+all"
	# optional
	inputs:
		- name: $input_name # name is optional
		  from: $input
		  with:
			  $args
		  expose:
			  - use: $data
				as: $alias
				with:
					$args

	# optional
	outputs:
		- name: $output_name # name is optional
		  to: $output
		  with:
			  $args

	# optional
	hooks:
		- run: $hook_func
		  on: $hook_event
		  with:
			  $args

	# required
	steps:
		- do: $step
		  with:
			  $args
		  inputs:
			- from: $input_name
			  use: $data
			  with:
				  $args
			  as: $alias
		  after: $other_steps
```

This feels more consistent.
