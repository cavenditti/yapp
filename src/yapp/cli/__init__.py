import argparse
import yaml
import graphlib
import importlib.util
import os
import types
import inspect
import re
import logging
from types import MethodType

from yapp.core import Pipeline, Job, Inputs
from yapp import ConfigurationError
from yapp.cli.validation import pipelines_schema_validation
from yapp.cli.logs import setup_logging


def env_constructor(loader, node):
    """
    Conctructor to automatically look up for env variables
    """
    try:
        value = loader.construct_scalar(node)
        return os.environ[value]
    except KeyError as e:
        raise KeyError(f"Missing environment variable: {e.args[0]}") from None


# use !env VARIABLENAME to refer env variables
yaml.add_constructor("!env", env_constructor)


def load_module(pipeline_name, module_name):
    logging.debug(
        f'Requested module to load "{module_name}" for pipeline "{pipeline_name}"'
    )
    base_paths = [pipeline_name, "./"]
    # Remove eventual trailing ".py" and split at dots
    ref = re.sub(r"\.py$", "", module_name).split(".")
    # return a possible path for each base_path
    paths = [os.path.join(*[base_path] + ref) for base_path in base_paths]
    # try to load from all possible paths
    loaded = False
    for path in paths:
        try:
            logging.debug(f"Trying path {path} for module {module_name}")
            spec = importlib.util.spec_from_file_location(module_name, path + ".py")
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
            raise FileNotFoundError(f"Cannot locate module {module_name} at {paths}") from None

    logging.debug(f"Found module {module}")
    return module


def build_job(pipeline_name, step):
    """
    Create Job given pipeline and step name
    """
    logging.debug(f'Building job "{step}" for pipeline "{pipeline_name}"')

    # TODO make this code less ugly
    try:
        module = load_module(pipeline_name, step)
        func_name = "execute"
    except FileNotFoundError:
        # maybe it's a function in module, try also loading that
        if "." in step:
            module_name, func_name = step.rsplit(".", 1)
            module = load_module(pipeline_name, module_name)

    # Try first loading a Job from the module,
    # if there's none try with execute function
    # if there's none try loading a function from module
    try:
        job = getattr(module, step)
        logging.debug(f"Using Job object {step} from {module}")
    except AttributeError:
        # TODO check again this code
        inner_fn = getattr(module, func_name)
        logging.debug(f'Using function "{func_name}" from {module}')

        # Writing the execute function to use the loaded function and making it a bound method
        # of a new created Job subclass. This is black magic.
        params = list(inspect.signature(inner_fn).parameters.keys())
        func_body = f"""def execute (self, {','.join(params)}):
                    return inner_fn({','.join(params)})
                    """
        # logging.debug(f'Function code: {func_body}')
        step_code = compile(func_body, step, "exec")
        step_func = types.FunctionType(step_code.co_consts[0], locals(), step)

        # class execution namespace
        def clsexec(ns):
            ns["__module__"] = "yapp.jobs"
            return ns

        # It's not actually required to have our new Job subclass to REALLY be a Job subclass
        # since the execute function is not going to access self anyway, but we may decide in future
        # to move some logic from Pipeline inside the Job, or add whatever functionality.
        # So doing things this way may make things easier in the future.
        class ConcreteJob(Job):
            def execute(self, *inputs):
                pass

        # Create new Job subclass
        NewJob = types.new_class(step, bases=(ConcreteJob,), exec_body=clsexec)
        NewJob.execute = MethodType(step_func, NewJob)
        # logging.debug(inspect.signature(NewJob.execute))
        NewJob = Job.register(NewJob)
    return NewJob


def build_pipeline(pipeline_name, pipeline_cfg, inputs=None, outputs=None, hooks={}):
    """
    Creates pipeline from pipeline and config definition dicts
    """

    def make_dag(step_list):
        """
        Create DAG dictionary suitable for topological ordering from configuration parsing output
        """
        dag = {}
        for step in step_list:
            logging.debug(f'<steps> parsing "{step}"')
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
    logging.debug(f'Performing topological ordering on steps: "{steps}"')
    try:
        ordered_steps = graphlib.TopologicalSorter(steps).static_order()
        ordered_steps = list(ordered_steps)
    except graphlib.CycleError:
        raise graphlib.CycleError(
            f"Invalid pipeline definition {pipeline_name}: cycle in steps dependencies"
        ) from None
    logging.debug(f"Successfully ordered steps: {ordered_steps}")

    # First step should be None: that is there are no dependencies for first step
    assert ordered_steps[0] is None

    # for each step get the source and load it
    jobs = [
        build_job(pipeline_name, step) for step in ordered_steps[1:]
    ]  # ignoring first None
    return Pipeline(jobs, name=pipeline_name, inputs=inputs, outputs=outputs, **hooks)


def create_adapter(pipeline_name, adapter_def):
    """
    Loads the relevant module and instantiates an adapter from it
    """
    # get adapter name and prepare contructor params (if any)
    params = {}
    if type(adapter_def) != str:  # dict
        adapter_name = next(iter(adapter_def))
        params = adapter_def[adapter_name]
    else:  # str
        adapter_name = adapter_def

    # load module and get attr Class
    # Should be in the form module.Class,
    # if not we assume the module has the same name as the class
    if "." in adapter_name:
        module_name, adapter_name = adapter_name.rsplit(".", 1)
    else:
        module_name, adapter_name = adapter_name, adapter_name
    module = load_module(pipeline_name, module_name)
    adapter_class = getattr(module, adapter_name)

    # instantiate adapter and return it
    return adapter_class(**params)


def build_inputs(pipeline_name, cfg_inputs, cfg_expose):
    """
    Sets up inputs from `inputs` and `expose` fields in YAML files
    """
    logging.debug("Starting input creation")

    sources = set()
    for input_def in cfg_inputs:
        logging.debug(f'<inputs> parsing "{input_def}"')

        adapter = create_adapter(pipeline_name, input_def)
        logging.debug(f"Created input adapter {adapter}")
        sources.add(adapter)
    inputs = Inputs(sources=sources)

    for expose_def in cfg_expose:
        logging.debug(f'<expose> parsing "{expose_def}"')
        key = next(iter(expose_def))
        for item in expose_def[key]:
            exposed_var = next(iter(item))
            if "." in item[exposed_var]:
                raise ValueError("Dots not allowed in exposed names")
            # FIXME better keep the whole name as key to avoid conflicts
            # needs to change Inputs class
            if "." in key:
                _, adapter_name = key.rsplit(".", 1)
            else:
                adapter_name = key
            inputs.expose(adapter_name, exposed_var, item[exposed_var])

    return inputs


def build_outputs(pipeline_name, cfg_outputs):
    """
    Sets up outputs from `outputs` field in YAML files
    """
    outputs = set()
    for output_def in cfg_outputs:
        logging.debug(f'<outputs> parsing "{output_def}"')

        adapter = create_adapter(pipeline_name, output_def)
        logging.debug(f"Created output adapter {adapter}")
        outputs.add(adapter)
    return outputs


def build_hooks(pipeline_name, cfg_hooks):
    """
    Sets up hooks from `hooks` field in YAML files
    """

    # If there are no hooks do nothing
    if not cfg_hooks:
        return {}

    hooks = {}
    for hook_tuple in cfg_hooks.items():
        logging.debug(f'<hooks> parsing "{hook_tuple}"')
        hook_name, hooks_list = hook_tuple

        # check if a valid hook
        if hook_name not in Pipeline._valid_hooks:
            raise ValueError(
                f"""Invalid hook specified: {hook_name}.
Hooks can be one of {list(Pipeline._hooks.keys())}"""
            )

        hooks[hook_name] = []

        # Then for each function in the list try to load it
        for hook in hooks_list:
            if type(hook) is not str:
                raise ValueError(f"Invalid hook value for {hook_name}: {hook}")
            module_name, func_name = hook.rsplit(".", 1)
            module = load_module(pipeline_name, module_name)
            func = getattr(module, func_name)
            logging.debug(f"Using function: {func_name} from module {module_name}")
            hooks[hook_name].append(func)
        logging.debug(f"AO: {hook_name} {hooks[hook_name]}")

    logging.debug(f'Parsed hooks: {hooks}"')
    return hooks


def yaml_read(path):
    """
    Read YAML from path
    """
    with open(path, "r") as file:
        parsed = yaml.full_load(file)
    return parsed


def create_pipeline(
    pipeline_name, path="./", pipelines_file="pipelines.yml", config_file="config.yml"
):
    """
    Reads pipelines.yml and config.yml to create a pipeline accordingly
    """

    # Valid pipeline fields
    valid_fields = {
        "steps",
        "inputs",
        "outputs",
        "expose",
        "hooks",
    }  # TODO add generic config
    # Auxiliary fields, not used for steps definition
    config_fields = valid_fields - {"steps"}

    pipelines_file = os.path.join(path, pipelines_file)
    config_file = os.path.join(path, config_file)

    # Read yaml configuration and validate it
    pipelines_yaml = yaml_read(pipelines_file)
    logging.debug(f"Loaded YAML: {pipelines_yaml}")
    config_errors = pipelines_schema_validation(pipelines_yaml)

    if pipeline_name in config_errors:
        raise ConfigurationError(config_errors, relevant_field=pipeline_name)
    elif config_errors:
        logging.warning(f'Configuration errors for pipelines: {list(config_errors.keys())}')
    else:
        logging.debug('Configuration OK')

    # Check if requested pipeline is in config and get only its config
    if pipeline_name not in pipelines_yaml:
        raise KeyError(f"Invalid pipeline name: {pipeline_name}")
    pipeline_cfg = pipelines_yaml[pipeline_name]

    # read global config
    try:
        config_yaml = yaml_read(config_file)
    except FileNotFoundError:
        logging.warning(f"Skipping missing config file {config_file}")
        config_yaml = {}

    logging.debug(f"pipelines_yaml: {pipeline_cfg}")
    logging.debug(f"[NOT IMPLEMENTED YET] config_yaml: {config_yaml}")

    # FIXME properly merge configurations
    # overwrite global with pipeline specific
    for field in config_fields:
        logging.debug(f"merging field {field}")
        # empty dictionaries if missing
        # config_yaml[field] = config_yaml.get(field, {})
        pipeline_cfg[field] = pipeline_cfg.get(field, [])
        """
        for value in pipeline_cfg[field]:
            logging.debug(f'pipeline field: {pipeline_cfg[field]} {value}')
            logging.debug(f'global field: {config_yaml[field]}')
            config_yaml[field][value] = (pipeline_cfg[field])
        """

    config_yaml = pipeline_cfg

    logging.debug(f"Using pipeline config: {config_yaml}")
    inputs = build_inputs(pipeline_name, config_yaml["inputs"], config_yaml["expose"])
    outputs = build_outputs(pipeline_name, config_yaml["outputs"])
    hooks = build_hooks(pipeline_name, config_yaml["hooks"])
    pipeline = build_pipeline(
        pipeline_name, pipeline_cfg, inputs=inputs, outputs=outputs, hooks=hooks
    )

    return pipeline


def main():
    parser = argparse.ArgumentParser(description="Run yapp pipeline")

    parser.add_argument(
        "-p",
        "--path",
        nargs="?",
        default="./",
        help="Path to look in for pipelines definitions",
    )

    parser.add_argument(
        "-d",
        "--debug",
        action="store_const",
        dest="loglevel",
        const="DEBUG",
        default="INFO",
        help="Set loglevel to DEBUG, same as --loglevel=DEBUG",
    )

    parser.add_argument(
        "-l",
        "--loglevel",
        nargs="?",
        dest="loglevel",
        default="INFO",
        help="Log level to use",
    )

    parser.add_argument(
        "-f",
        "--logfile",
        nargs="?",
        dest="logfile",
        type=str,
        default='',
        help="Log level to use",
    )

    parser.add_argument(
        '--color',
        action="store_const", dest="color", const=True,
        default=False,
        help='Print colored output for logs'
    )

    parser.add_argument(
        'pipeline',
        type=str,
        help='Pipeline name'
    )

    args = parser.parse_args()
    loglevel = args.loglevel.upper()

    setup_logging(loglevel, color=args.color, logfile=args.logfile)

    try:
        pipeline = create_pipeline(args.pipeline, path=args.path)
    except Exception as e:
        logging.exception(e)
        exit(-1)

    try:
        pipeline()
    except Exception as e:
        logging.exception(e)
        logging.debug(f"pipeline.inputs: {pipeline.inputs.__repr__()}")
        logging.debug(f"pipeline.outputs: {pipeline.outputs}")
        logging.debug(f"pipeline.job_list: {pipeline.job_list}")
        for job in pipeline.job_list:
            args = inspect.getfullargspec(job.execute).args
            logging.debug(f"{job}.execute arguments: {args}")
        exit(-1)


if __name__ == "__main__":
    main()
