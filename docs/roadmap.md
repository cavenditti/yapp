
### Ideas for a possible evolution in `1.x.x`

`pipelines.yml` schema now looks good.
There are some possible enhancements but I expect the changes to be mostly backward compatible from
now on.

## Planned features and changes:

 - Clear and detailed pipelines.yml specification
 - Better comprehensive docstrings
 - Better tests
 - Aliases for inputs, outputs and steps [in progress]
 - More flexible outputs (allow defining for which steps each output should be used)
 - A good and working example
 - Pipeline status monitor class example
 - Allow importing from Jupyter notebooks
 - Consider permitting repeted tasks in a single pipeline (can this be useful?)
 - For each step, keep track of the inputs required in future steps. So that unneeded ones can be removed from memory
 - Graph data flow between jobs
 - Automatic deployment to cloud
