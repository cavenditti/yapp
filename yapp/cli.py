import argparse
import yaml
import graphlib
import importlib.util
import os
import types
import re
from types import MethodType

from core import Pipeline, Job, Inputs


def env_constructor(loader, node):
    """
    Conctructor to automatically look up for env variables
    """
    try:
        value = loader.construct_scalar(node)
        return os.environ[value]
    except KeyError as e:
        raise KeyError(f'Missing environment variable: {e.args[0]}')


# use !env VARIABLENAME to refer env variables
yaml.add_constructor("!env", env_constructor)


def load_module(pipeline_name, module_name):
    base_paths = [pipeline_name, "./"]
    # Remove eventual trailing ".py" and split at dots
    ref = re.sub(r"\.py$", "", module_name).split(".")
    # return a possible path for each base_path
    paths = [os.path.join(*[base_path] + ref) for base_path in base_paths]
    # try to load from all possible paths
    loaded = False
    for path in paths:
        try:
            spec = importlib.util.spec_from_file_location(pipeline_name, path + ".py")
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            loaded = True
            break
        except FileNotFoundError:
            pass
    if not loaded:
        raise FileNotFoundError(f"Cannot locate module {module_name} at {paths}")
    return module


def build_job(pipeline_name, step):
    """
    Create Job given pipeline and step name
    """
    module = load_module(pipeline_name, step)
    # Try first loading a Job from the module,
    # if there's none try with execute function
    try:
        job = getattr(module, step)
    except AttributeError:
        # TODO check again this code
        inner_fn = getattr(module, "execute")

        def outer_fn(self, *inputs):
            return inner_fn(*inputs)

        # Create new Job subclass
        NewJob = types.new_class(step, bases=(Job,))
        NewJob.execute = MethodType(outer_fn, NewJob)
        job = NewJob
    return job


def build_pipeline(pipeline_name, pipeline_cfg, inputs=None, outputs=None, hooks={}):
    """
    Creates pipeline from pipeline and config definition dicts
    """

    def make_dag(step_list):
        """
        Create DAG dictionary suitable for topological ordering from yaml parsing output
        """
        dag = {}
        for step in step_list:
            if type(step) is str:
                dag[step] = {None}
            elif type(step) is dict:
                node = list(step.keys())[0]
                node_parents = step[node]
                if type(node_parents) is str:
                    dag[node] = {step[node]}
                else:
                    dag[node] = set(step[node])
        return dag

    steps = make_dag(pipeline_cfg["steps"])
    try:
        ordered_steps = graphlib.TopologicalSorter(steps).static_sort()
    except graphlib.CycleError:
        raise graphlib.CycleError(f"Invalid pipeline definition {pipeline_name}: cycle in steps dependencies")

    # for each step get the source and load it
    jobs = [build_job(pipeline_name, step) for step in ordered_steps]
    return Pipeline(jobs, inputs=inputs, outputs=outputs, **hooks)


def build_inputs(pipeline_name, yaml_input, yaml_expose):
    """
    Sets up inputs from `inputs` and `expose` fields in YAML files
    """
    sources = set()
    for input_def in yaml_input:
        input_name = next(iter(input_def))
        module = load_module(pipeline_name, input_name)
        sources.add(getattr(module, input_name))
    inputs = Inputs(sources=sources)
    for expose_def in yaml_expose:
        key = next(iter(expose_def))
        for item in expose_def[key]:
            exposed_var = next(iter(item))
            inputs.expose(exposed_var, item[exposed_var])
    return inputs


def build_outputs(pipeline_name, yaml_outputs):
    """
    Sets up outputs from `outputs` field in YAML files
    """
    raise NotImplementedError


def build_hooks(pipeline_name, yaml_hooks):
    """
    Sets up hooks from `hooks` field in YAML files
    """
    raise NotImplementedError


def create_pipeline(pipeline_name, path="./", pipelines_file="pipelines.yml", config_file="config.yml"):
    """
    Reads pipelines.yml and config.yml to create a pipeline accordingly
    """

    pipelines_file = os.path.join(path, pipelines_file)
    config_file = os.path.join(path, config_file)

    with open(pipelines_file, "r") as file:
        pipelines_yaml = yaml.safe_load(file)
    if pipeline_name not in pipelines_yaml:
        raise KeyError(f"Invalid pipeline name: {pipeline_name}")

    pipeline_cfg = pipelines_yaml[pipeline_name]

    with open(config_file, "r") as file:
        config_yaml = yaml.safe_load(file)

    # Valid pipeline fields
    valid_fields = {"steps", "inputs", "outputs", "expose", "hooks"}  # TODO add generic config
    # Auxiliary fields, not used for steps definition
    config_fields = valid_fields - {"steps"}
    # overwrite global with pipeline specific
    for field in config_fields:
        config_yaml[field].update(pipeline_cfg[field])

    inputs = build_inputs(config_yaml["inputs"], config_yaml["expose"])
    outputs = build_outputs(config_yaml["outputs"])
    hooks = build_hooks(config_yaml["hooks"])
    pipeline = build_pipeline(pipeline_name, pipeline_cfg, inputs=inputs, outputs=outputs, hooks=hooks)

    return pipeline


def main():
    parser = argparse.ArgumentParser(description='Run yapp pipeline')
    parser.add_argument('pipeline', type=str,
                        help='Pipeline name')
    parser.add_argument('path', nargs='?', default='./',
                        help='Path to look in for pipelines definitions')

    args = parser.parse_args()
    pipe_name = args.pipeline
    path = args.path

    pipeline = create_pipeline(pipe_name, path=path)
    pipeline()


if __name__ == "__main__":
    main()
