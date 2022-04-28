import graphlib
import importlib.util
import inspect
import logging
import os
import re
import types
from types import MethodType

import yaml

from yapp.core.errors import ConfigurationError
from yapp.cli.validation import validate
from yapp.core import Inputs, Job, Pipeline


def env_constructor(loader, node):
    """
    Conctructor to automatically look up for env variables
    """
    try:
        value = loader.construct_scalar(node)
        return os.environ[value]
    except KeyError as error:
        raise KeyError(f"Missing environment variable: {error.args[0]}") from None


def yaml_read(path):
    """
    Read YAML from path
    """
    # use !env VARIABLENAME to refer env variables
    yaml.add_constructor("!env", env_constructor)

    with open(path, "r", encoding="utf-8") as file:
        parsed = yaml.full_load(file)
    return parsed


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
    }  # TODO add generic config directly using "config"
    # Auxiliary fields, not used for steps definition
    config_fields = valid_fields - {"steps"}

    dict_fields = ["hooks"]

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
        # return a possible path for each base_path
        paths = [os.path.join(*[base_path] + ref) for base_path in self.base_paths]
        # try to load from all possible paths
        for path in paths:
            try:
                logging.debug("Trying path %s for module %s", path, module_name)
                spec = importlib.util.spec_from_file_location(module_name, path + ".py")
                module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(module)
                break
            except FileNotFoundError:
                pass
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

    def build_job(self, step):  # pylint: disable=no-self-use
        """
        Create Job given pipeline and step name
        """
        logging.debug('Building job "%s" for pipeline "%s"', step, self.pipeline_name)

        try:
            module = self.load_module(step)
            func_name = "execute"
        except FileNotFoundError:
            # maybe it's a function in module, try also loading that
            if "." in step:
                module_name, func_name = step.rsplit(".", 1)
                module = self.load_module(module_name)

        # Try first loading a Job from the module,
        # if there's none try with execute function
        # if there's none try loading a function from module
        try:
            job = getattr(module, step)
            logging.debug("Using Job object %s from %s", step, module)
            return job
        except AttributeError:
            inner_fn = getattr(module, func_name)
            logging.debug('Using function "%s" from %s', func_name, module)

            # Writing the execute function to use the loaded function and making it a bound method
            # of a new created Job subclass. This is black magic.
            params = list(inspect.signature(inner_fn).parameters.keys())
            func_body = f"""def execute (self, {','.join(params)}):
                        return inner_fn({','.join(params)})
                        """
            # logging.debug('Function code: %s', func_body)
            step_code = compile(func_body, step, "exec")
            step_func = types.FunctionType(step_code.co_consts[0], locals(), step)

            # class execution namespace
            def clsexec(namespace):
                namespace["__module__"] = "yapp.jobs"
                return namespace

            # It's not actually required to have our new Job subclass to really be a Job subclass
            # since the execute function is not going to access self anyway, but we may decide in future
            # to move some logic from Pipeline inside the Job, or add whatever functionality.
            # So doing things this way may make things easier in the future.
            class ConcreteJob(Job):  # pylint: disable=missing-class-docstring
                def execute(self, *inputs):
                    pass

            # Create new Job subclass
            new_job_class = types.new_class(
                step, bases=(ConcreteJob,), exec_body=clsexec
            )
            new_job_class.execute = MethodType(step_func, new_job_class)
            # logging.debug(inspect.signature(new_job_class.execute))
            new_job_class = Job.register(new_job_class)
        return new_job_class

    def build_pipeline(self, pipeline_cfg, inputs=None, outputs=None, hooks=None):
        """
        Creates pipeline from pipeline and config definition dicts
        """

        def make_dag(step_list):
            """
            Create DAG dictionary suitable for topological ordering from configuration parsing output
            """
            dag = {}
            for step in step_list:
                logging.debug('<steps> parsing "%s"', step)
                # make strings just like the others
                if isinstance(step, str):
                    step = {step: {None}}
                node = list(step.keys())[0]
                node_parents = step[node]
                if isinstance(node_parents, str):
                    dag[node] = {step[node]}
                else:
                    dag[node] = set(step[node])
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
        assert ordered_steps[0] is None

        # for each step get the source and load it
        jobs = [
            self.build_job(step) for step in ordered_steps[1:]
        ]  # ignoring first None

        if not hooks:
            hooks = {}

        return Pipeline(
            jobs, name=self.pipeline_name, inputs=inputs, outputs=outputs, **hooks
        )

    def create_adapter(self, adapter_def):
        """
        Loads the relevant module and instantiates an adapter from it
        """
        # get adapter name and prepare contructor params (if any)
        params = {}
        if isinstance(adapter_def, dict):
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

    def build_inputs(self, cfg_inputs, cfg_expose):
        """
        Sets up inputs from `inputs` and `expose` fields in YAML files
        """
        logging.debug("Starting input creation")

        sources = set()
        for input_def in cfg_inputs:
            logging.debug('<inputs> parsing "%s"', input_def)

            adapter = self.create_adapter(input_def)
            logging.debug("Created input adapter %s", adapter)
            sources.add(adapter)
        inputs = Inputs(sources=sources)

        for expose_def in cfg_expose:
            logging.debug('<expose> parsing "%s"', expose_def)
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

    def build_outputs(self, cfg_outputs):
        """
        Sets up outputs from `outputs` field in YAML files
        """
        outputs = set()
        for output_def in cfg_outputs:
            logging.debug('<outputs> parsing "%s"', output_def)

            adapter = self.create_adapter(output_def)
            logging.debug("Created output adapter %s", adapter)
            outputs.add(adapter)
        return outputs

    def build_hooks(self, cfg_hooks):
        """
        Sets up hooks from `hooks` field in YAML files
        """

        # If there are no hooks do nothing
        if not cfg_hooks:
            return {}

        hooks = {}
        for hook_tuple in cfg_hooks.items():
            logging.debug('<hooks> parsing "%s"', hook_tuple)
            hook_name, hooks_list = hook_tuple

            # check if a valid hook
            if hook_name not in Pipeline.valid_hooks:
                raise ValueError(
                    f"""Invalid hook specified: {hook_name}.
    Hooks can be one of {Pipeline.valid_hooks}"""
                )

            hooks[hook_name] = []

            # Then for each function in the list try to load it
            for hook in hooks_list:
                module_name, func_name = hook.rsplit(".", 1)
                module = self.load_module(module_name)
                func = getattr(module, func_name)
                logging.debug(
                    "Using function: %s from module %s", func_name, module_name
                )
                hooks[hook_name].append(func)
            logging.debug("AO: %s %s", hook_name, hooks[hook_name])

        logging.debug('Parsed hooks: %s"', hooks)
        return hooks

    def parse(self):
        """
        Reads and parses pipelines.yml, creates a pipeline object
        """

        # Read yaml configuration and validate it
        pipelines_yaml = yaml_read(self.pipelines_file)
        logging.debug("Loaded YAML: %s", pipelines_yaml)
        config_errors = validate(pipelines_yaml)

        if config_errors:
            logging.warning(
                "Configuration errors for pipelines: %s", list(config_errors.keys())
            )
            if self.pipeline_name in config_errors:
                raise ConfigurationError(
                    config_errors, relevant_field=self.pipeline_name
                )
        else:
            logging.debug("Configuration OK")

        # Check if requested pipeline is in config and get only its config
        if self.pipeline_name not in pipelines_yaml:
            raise KeyError(f"Invalid pipeline name: {self.pipeline_name}")
        pipeline_cfg = pipelines_yaml[self.pipeline_name]

        # read global config
        try:
            cfg = pipelines_yaml.pop("+all")
        except KeyError:
            logging.debug("'+all' not found: no global configuration defined.")
            cfg = {}

        logging.debug("Starting config merge")
        logging.debug("Pipeline config: %s", pipeline_cfg)
        logging.debug("Global config: %s", cfg)

        # used to merge fields from specific and global configurations
        def merge_field(dict_a: dict, dict_b: dict, field: str) -> dict:
            """
            Appends the list dict_b[field] to the list dict_a[field] and returns dict_a
            """
            dict_a[field] = dict_a.get(field, [])
            b_list = dict_b.get(field, [])
            dict_a[field] += b_list
            return dict_a

        # overwrite global with pipeline specific
        for field in ConfigParser.config_fields:
            logging.debug("merging field %s", field)
            # empty dictionaries if missing
            logging.debug(
                "pipeline field: %s",
                pipeline_cfg[field] if field in pipeline_cfg else "missing",
            )
            logging.debug("global field: %s", cfg[field] if field in cfg else "missing")
            if field in ConfigParser.dict_fields:
                cfg[field] = cfg.get(field, {})
                pipeline_cfg[field] = pipeline_cfg.get(field, {})
                for subfield in pipeline_cfg[field]:
                    cfg[field] = merge_field(cfg[field], pipeline_cfg[field], subfield)
                cfg[field].update(pipeline_cfg[field])
            else:
                cfg = merge_field(cfg, pipeline_cfg, field)
            logging.debug("merged field: %s", cfg[field])

        logging.debug("Merged pipeline config: %s", cfg)

        # Building objects
        inputs = self.build_inputs(cfg["inputs"], cfg["expose"])
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
