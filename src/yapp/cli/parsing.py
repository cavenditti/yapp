import graphlib
import importlib.util
import inspect
import logging
import os
import re
import types
from collections import defaultdict
from types import MethodType

import yaml

from yapp.cli.validation import validate
from yapp.core import Inputs, Job, Pipeline
from yapp.core.errors import (
    ConfigurationError,
    ImportedCodeFailed,
    MissingConfiguration,
    MissingEnv,
    MissingPipeline,
)


def camel_to_snake(name):
    """Returns snake_case version of a CamelCase string"""
    name = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name)
    return re.sub("([a-z0-9])([A-Z])", r"\1_\2", name).lower()


def env_constructor(loader, node):
    """
    Conctructor to automatically look up for env variables
    """
    try:
        value = loader.construct_scalar(node)
        return os.environ[value]
    except KeyError as error:
        raise MissingEnv(error.args[0]) from None


def do_nothing_constructor(self, node):
    """
    Constructor just returning the string for the node
    """
    return self.construct_scalar(node)


def yaml_read(path):
    """
    Read YAML from path
    """
    # use !env VARIABLENAME to refer env variables
    yaml.add_constructor("!env", env_constructor)

    # Disable awful YAML 1.1 behaviour on booleans-lookalike
    # If I write 'on' I want 'on', not boolean True
    yaml.add_constructor("tag:yaml.org,2002:bool", do_nothing_constructor)

    try:
        with open(path, "r", encoding="utf-8") as file:
            parsed = yaml.full_load(file)
        return parsed
    except FileNotFoundError:
        raise MissingConfiguration() from None


class ConfigParser:
    """
    Parses config files and build a pipeline accordingly
    """

    # Valid pipeline fields
    valid_fields = {
        "steps",
        "inputs",
        "outputs",
        "expose",
        "hooks",
        "monitor",
        "config",
    }  # TODO add generic config directly using "config"
    # Auxiliary fields, not used for steps definition
    config_fields = valid_fields - {"steps", "config"}

    def __init__(self, pipeline_name, path="./", pipelines_file="pipelines.yml"):
        self.pipeline_name = pipeline_name
        self.path = path

        self.pipelines_file = pipelines_file
        self.pipelines_file = os.path.join(path, pipelines_file)

        self.base_paths = [os.path.join(path, self.pipeline_name), path]

    def load_module(self, module_name):
        """
        Loads a python module from a .py file or yapp modules
        """
        logging.debug(
            'Requested module to load "%s" for pipeline "%s"',
            module_name,
            self.pipeline_name,
        )
        # Remove eventual trailing ".py" and split at dots
        ref = re.sub(r"\.py$", "", module_name).split(".")
        # same but snake_case
        ref_snake = re.sub(r"\.py$", "", module_name).split(".")
        # return a possible path for each base_path for both possible names
        paths = [os.path.join(*[base_path] + ref) for base_path in self.base_paths]
        paths += [
            os.path.join(*[base_path] + ref_snake) for base_path in self.base_paths
        ]
        # try to load from all possible paths
        for path in paths:
            logging.debug("Trying path %s for module %s", path, module_name)
            try:
                spec = importlib.util.spec_from_file_location(module_name, path + ".py")
            except FileNotFoundError:
                continue

            if not spec:
                continue

            module = importlib.util.module_from_spec(spec)

            try:
                spec.loader.exec_module(module)
            except FileNotFoundError:
                # always continue on FileNotFoundError
                continue
            except Exception as error:
                raise ImportedCodeFailed(module, *error.args) from None

            # if everything goes well stop here and don't try next possibilities
            break
        else:
            # if didn't find it, try from yapp
            # and if still cannot find it, raise an error
            try:
                return importlib.import_module(f"yapp.adapters.{module_name}")
            except ModuleNotFoundError:
                raise FileNotFoundError(
                    f"Cannot locate module {module_name} at {paths}"
                ) from None

        logging.debug("Found module %s", module)
        return module

    def build_new_job_class(
        self, step, module, func_name, params
    ):  # pylint: disable=no-self-use
        """
        Build new Job subclass at runtime
        """
        inner_fn = getattr(module, func_name)
        logging.debug('Using function "%s" from %s', func_name, module)

        # Writing the execute function to use the loaded function and making it a bound method
        # of a new created Job subclass. This is black magic.

        # get args and kwargs from inner_fn
        args = list(inspect.signature(inner_fn).parameters.keys())
        full_args = map(str, inspect.signature(inner_fn).parameters.values())
        inner_args = map("=".join, zip(args, args))

        func = f"""def execute (self, {', '.join(full_args)}):
                    return inner_fn({','.join(inner_args)})
                    """
        # logging.debug('Function code: %s', func_body)
        func = compile(func, step, "exec")

        arg_spec = inspect.getfullargspec(inner_fn)
        position = len(arg_spec.defaults) if arg_spec.defaults else 0
        func = types.FunctionType(
            func.co_consts[position], locals(), step, arg_spec.defaults
        )

        # class execution namespace
        def clsexec(namespace):
            namespace["__module__"] = "yapp.jobs"
            return namespace

        # It's not actually required to have our new Job subclass to really be a Job subclass
        # since the execute function is not going to access self anyway, but we may decide in future
        # to move some logic from Pipeline inside the Job, or add whatever functionality.
        # So doing things this way may make things easier in the future.
        class ConcreteJob(Job):  # pylint: disable=missing-class-docstring
            def execute(self, *inputs, **params):
                pass

        # Create new Job subclass
        new_job_class = types.new_class(step, bases=(ConcreteJob,), exec_body=clsexec)
        new_job_class.execute = MethodType(func, new_job_class)
        # logging.debug(inspect.signature(new_job_class.execute))
        new_job_class = Job.register(new_job_class)

        # assign parameters and assign job to return
        new_job_class.params = params
        return new_job_class

    def build_job(self, step, params):  # pylint: disable=no-self-use
        """
        Create Job given pipeline and step name
        """
        logging.debug('Building job "%s" for pipeline "%s"', step, self.pipeline_name)

        func_name = "execute"
        module = None

        try:
            module = self.load_module(step)
        except FileNotFoundError:
            # maybe it's a function in module, try also loading that
            if "." in step:
                module_name, func_name = step.rsplit(".", 1)
                module = self.load_module(module_name)

        if module is None:
            raise FileNotFoundError(f"Cannot load module {module}")

        # Try first loading a Job from the module,
        # if there's none try with execute function
        # if there's none try loading a function from module
        try:
            job = getattr(module, step)
            job.params = params
            logging.debug("Using Job object %s from %s", step, module)
        except AttributeError:
            job = self.build_new_job_class(step, module, func_name, params)

        # check for invalid kwargs
        arg_spec = inspect.getfullargspec(job.execute)
        if arg_spec.defaults:
            kwargs = arg_spec.args[-len(arg_spec.defaults) :]
        else:
            kwargs = []
        for single_param in params:
            if single_param not in kwargs:
                raise ConfigurationError(
                    f'Job {step} does not take a "{single_param}" argument'
                )

        return job

    def build_pipeline(self, pipeline_cfg, inputs=None, outputs=None, hooks=None):
        """
        Creates pipeline from pipeline and config definition dicts
        """

        params_mapping = {}

        def make_dag(step_list):
            """
            Create DAG dictionary suitable for topological ordering from configuration parsing output
            """
            dag = {}
            for step in step_list:
                logging.debug('<steps> parsing "%s"', step)
                # make strings just like the others
                after = step.get("after", [])
                if isinstance(after, str):
                    after = [after]

                dag[step["run"]] = set(after)
                params_mapping[step["run"]] = step.get("with", {})
            return dag

        steps = make_dag(pipeline_cfg["steps"])
        logging.debug('Performing topological ordering on steps: "%s"', steps)
        try:
            ordered_steps = graphlib.TopologicalSorter(steps).static_order()
            ordered_steps = list(ordered_steps)
        except graphlib.CycleError:
            raise graphlib.CycleError(
                f"Invalid pipeline definition {self.pipeline_name}: cycle in steps dependencies"
            ) from None
        logging.debug("Successfully ordered steps: %s", ordered_steps)

        # First step should be None: that is there are no dependencies for first step
        # assert ordered_steps[0] is None

        # for each step get the source and load it
        jobs = [self.build_job(step, params_mapping[step]) for step in ordered_steps]

        if not hooks:
            hooks = {}

        return Pipeline(
            jobs, name=self.pipeline_name, inputs=inputs, outputs=outputs, **hooks
        )

    def create_adapter(self, adapter_name: str, params: dict):
        """
        Loads the relevant module and instantiates an adapter from it
        """

        # load module and get attr Class
        # Should be in the form module.Class,
        # if not we assume the module has the same name as the class
        if "." in adapter_name:
            module_name, adapter_name = adapter_name.rsplit(".", 1)
        else:
            module_name = adapter_name
        module = self.load_module(module_name)
        adapter_class = getattr(module, adapter_name)

        # instantiate adapter and return it
        logging.debug(params)
        try:
            args = params.pop("+args")
        except KeyError:
            args = []
        return adapter_class(*args, **params)

    def make_input(self, single_input: dict):
        """Create a single input from its dict Configuration"""
        logging.debug('<inputs> parsing "%s"', single_input)
        adapter_name = single_input["from"]
        params = single_input.get("with", {})
        expose_list = single_input.get("expose", [])

        input_adapter = self.create_adapter(adapter_name, params)

        logging.debug("Created input adapter %s", input_adapter)
        return input_adapter, expose_list

    def build_inputs(self, cfg_inputs, config=None):
        """
        Sets up inputs from `inputs` and `expose` fields in YAML files
        """
        logging.debug("Starting input creation")

        sources = set()
        exposed = {}

        for input_def in cfg_inputs:
            adapter, exposed_list = self.make_input(input_def)
            sources.add(adapter)
            exposed[adapter.__class__.__name__] = exposed_list

        inputs = Inputs(sources=sources, config=config)

        for name, expose_dict in exposed.items():
            for to_expose in expose_dict:
                logging.debug(
                    "Exposing %s %s %s", name, to_expose["use"], to_expose["as"]
                )
                inputs.expose(name, to_expose["use"], to_expose["as"])

        return inputs

    def make_output(self, single_output: dict):
        """Create a single output from its dict Configuration"""
        logging.debug('<outputs> parsing "%s"', single_output)
        adapter_name = single_output["to"]
        params = single_output.get("with", {})

        adapter = self.create_adapter(adapter_name, params)
        logging.debug("Created output adapter %s", adapter)
        return adapter

    def build_outputs(self, cfg_outputs):
        """
        Sets up outputs from `outputs` field in YAML files
        """
        outputs = set()
        for output_def in cfg_outputs:
            adapter = self.make_output(output_def)
            outputs.add(adapter)
        return outputs

    def make_hook(self, single_hook: dict):
        """Create a single hook from its dict Configuration"""
        run = single_hook["run"]
        hook_name = single_hook["on"]

        # check if a valid hook
        if hook_name not in Pipeline.VALID_HOOKS:
            raise ValueError(
                f"""Invalid hook specified: {hook_name}.
Hooks can be one of {Pipeline.VALID_HOOKS}"""
            )

        module_name, func_name = run.rsplit(".", 1)
        module = self.load_module(module_name)
        func = getattr(module, func_name)
        logging.debug(
            "Using function: %s from module %s as %s", func_name, module_name, hook_name
        )
        return hook_name, func

    def build_hooks(self, cfg_hooks):
        """
        Sets up hooks from `hooks` field in YAML files
        """

        # If there are no hooks do nothing
        if not cfg_hooks:
            return {}

        hooks = defaultdict(list)

        for hook_dict in cfg_hooks:
            logging.debug('<hooks> parsing "%s"', hook_dict)
            hook_name, func = self.make_hook(hook_dict)
            hooks[hook_name].append(func)

        logging.debug('Parsed hooks: %s"', hooks)
        return hooks

    def do_validation(self, pipelines_yaml: dict):
        """
        Performs validation on a dict read from a pipelines.yml file

        Raises:
            YappFatalError:
                On invalid configuration

        Args:
            pipelines_yaml (dict): pipelines_yaml
        """
        config_errors = validate(pipelines_yaml)

        if config_errors:
            logging.error(
                "Configuration errors for pipelines: %s", list(config_errors.keys())
            )
            if self.pipeline_name in config_errors:
                raise ConfigurationError(
                    config_errors, relevant_field=self.pipeline_name
                )
        else:
            logging.debug("Configuration OK")

    def parse(self, skip_validation=False):
        """
        Reads and parses pipelines.yml, creates a pipeline object
        """

        # Read yaml configuration and validate it
        pipelines_yaml = yaml_read(self.pipelines_file)
        logging.debug("Loaded YAML: %s", pipelines_yaml)

        if not skip_validation:
            self.do_validation(pipelines_yaml)

        # Check if requested pipeline is in definitions and get only its definitions
        if self.pipeline_name not in pipelines_yaml:
            raise MissingPipeline(self.pipeline_name)
        pipeline_cfg = pipelines_yaml[self.pipeline_name]

        # read global definitions
        global_config = {}  # used for `config` tag
        try:
            cfg = pipelines_yaml.pop("+all")
            global_config = cfg.pop("config")
        except KeyError:
            logging.debug("'+all' not found: no global configuration defined.")
            cfg = {}

        # make a defaultdict to simplify missing fields handling
        cfg = defaultdict(list, cfg)

        logging.debug("Starting definitions merge")
        logging.debug("Pipeline definitions: %s", pipeline_cfg)
        logging.debug("Global definitions: %s", cfg)

        # overwrite global with pipeline specific
        for field in ConfigParser.config_fields:
            logging.debug("merging field %s", field)
            logging.debug(
                "pipeline field: %s",
                pipeline_cfg[field] if field in pipeline_cfg else "missing",
            )
            logging.debug("global field: %s", cfg[field] if field in cfg else "missing")
            # actual definitions merging
            cfg[field] += pipeline_cfg.get(field, [])
            logging.debug("merged field: %s", cfg[field])

        logging.debug("Merged pipeline definitions: %s", cfg)

        pipeline_config = pipeline_cfg.get("config", {})
        global_config.update(pipeline_config)

        # Building objects
        inputs = self.build_inputs(cfg["inputs"], global_config)
        outputs = self.build_outputs(cfg["outputs"])
        hooks = self.build_hooks(cfg["hooks"])
        pipeline = self.build_pipeline(
            pipeline_cfg, inputs=inputs, outputs=outputs, hooks=hooks
        )

        return pipeline

    def switch_workdir(self, workdir=None):
        """
        Switches to the pipeline workdir that jobs and hooks expect
        """
        if not workdir:
            workdir = self.path
        os.chdir(workdir)
