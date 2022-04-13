import argparse
import yaml
import graphlib
import importlib.util
import os
import types
import re
import logging
from types import MethodType

from .core import Pipeline, Job, Inputs


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
        # if didn't find it, try from yapp
        # and if still cannot find it, raise an error
        try:
            return importlib.import_module(f"yapp.adapters.{module_name}")
        except ModuleNotFoundError:
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


def create_adapter(pipeline_name, adapter_def):
    # get adapter name and prepare contructor params (if any)
    params = {}
    if type(adapter_def) != str:  # dict
        adapter_name = next(iter(adapter_def))
        for d in adapter_def[adapter_name]:
            params.update(d)
    else:  # str
        adapter_name = adapter_def

    # load module and get attr Class
    # Should be in the form module.Class,
    # if not we assume the module has the same name as the class
    if '.' in adapter_name:
        module_name, adapter_name = adapter_name.rsplit('.', 1)
    else:
        module_name, adapter_name = adapter_name, adapter_name
    module = load_module(pipeline_name, module_name)
    adapter_class = getattr(module, adapter_name)

    # instantiate adapter and return it
    return adapter_class(**params)


def build_inputs(pipeline_name, yaml_inputs, yaml_expose):
    """
    Sets up inputs from `inputs` and `expose` fields in YAML files
    """
    logging.debug('Starting input creation')

    sources = set()
    for input_def in yaml_inputs:
        logging.debug(f'<inputs> parsing {input_def}')

        adapter = create_adapter(pipeline_name, input_def)
        logging.debug(f'Created input adapter {adapter}')
        sources.add(adapter)
    inputs = Inputs(sources=sources)

    for expose_def in yaml_expose:
        logging.debug(f'<expose> parsing {expose_def}')
        key = next(iter(expose_def))
        for item in expose_def[key]:
            exposed_var = next(iter(item))
            if '.' in item[exposed_var]:
                raise ValueError('Dots not allowed in exposed names')
            inputs.expose(key, exposed_var, item[exposed_var])

    return inputs


def build_outputs(pipeline_name, yaml_outputs):
    """
    Sets up outputs from `outputs` field in YAML files
    """
    outputs = set()
    for output_def in yaml_outputs:
        logging.debug(f'<outputs> parsing {output_def}')

        adapter = create_adapter(pipeline_name, output_def)
        logging.debug(f'Created output adapter {adapter}')
        outputs.add(adapter)
    return outputs


def build_hooks(pipeline_name, yaml_hooks):
    """
    Sets up hooks from `hooks` field in YAML files
    """
    raise NotImplementedError


def create_pipeline(pipeline_name, path="./", pipelines_file="pipelines.yml", config_file="config.yml"):
    """
    Reads pipelines.yml and config.yml to create a pipeline accordingly
    """

    # Valid pipeline fields
    valid_fields = {"steps", "inputs", "outputs", "expose", "hooks"}  # TODO add generic config
    # Auxiliary fields, not used for steps definition
    config_fields = valid_fields - {"steps"}

    pipelines_file = os.path.join(path, pipelines_file)
    config_file = os.path.join(path, config_file)

    with open(pipelines_file, "r") as file:
        pipelines_yaml = yaml.safe_load(file)
    if pipeline_name not in pipelines_yaml:
        raise KeyError(f"Invalid pipeline name: {pipeline_name}")

    pipeline_cfg = pipelines_yaml[pipeline_name]

    try:
        with open(config_file, "r") as file:
            config_yaml = yaml.safe_load(file)
    except FileNotFoundError:
        logging.info(f'Skipping missing config file {config_file}')
        config_yaml = {}

    logging.debug(f'pipelines_yaml: {pipeline_cfg}')
    logging.debug(f'[NOT IMPLEMENTED YET] config_yaml: {config_yaml}')

    # FIXME properly merge configurations
    # overwrite global with pipeline specific
    for field in config_fields:
        logging.debug(f'merging field {field}')
        # empty dictionaries if missing
        #config_yaml[field] = config_yaml.get(field, {})
        pipeline_cfg[field] = pipeline_cfg.get(field, [])
        """
        for value in pipeline_cfg[field]:
            logging.debug(f'pipeline field: {pipeline_cfg[field]} {value}')
            logging.debug(f'global field: {config_yaml[field]}')
            config_yaml[field][value] = (pipeline_cfg[field])
        """

    config_yaml = pipeline_cfg


    logging.info(f'Using pipeline config: {config_yaml}')
    inputs = build_inputs(pipeline_name, config_yaml["inputs"], config_yaml["expose"])
    outputs = build_outputs(pipeline_name, config_yaml["outputs"])
    hooks = build_hooks(pipeline_name, config_yaml["hooks"])
    pipeline = build_pipeline(pipeline_name, pipeline_cfg, inputs=inputs, outputs=outputs, hooks=hooks)

    return pipeline


def main():
    parser = argparse.ArgumentParser(description='Run yapp pipeline')
    parser.add_argument('--path', nargs='?', default='./',
                        help='Path to look in for pipelines definitions')
    parser.add_argument('--loglevel', nargs='?', default='INFO',
                        help='Log level to use')
    parser.add_argument('pipeline', type=str,
                        help='Pipeline name')


    args = parser.parse_args()

    logging_format = '%(levelname)-7s|%(module)-8s%(lineno)-4s %(funcName)-20s %(message)s'
    logging.basicConfig(format=logging_format, level=getattr(logging,args.loglevel), force=True)

    try:
        pipeline = create_pipeline(args.pipeline, path=args.path)
        pipeline()
    except Exception as e:
        logging.exception(e)


if __name__ == "__main__":
    main()
