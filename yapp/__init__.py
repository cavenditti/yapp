from yapp.Job import Job
from yapp.Pipeline import Pipeline,compose_pipeline

import yaml
import graphlib
import importlib.util
import os.path
import os.environ
import types
import re
from types import MethodType



def env_constructor(loader, node):
    """
    Conctructor to automatically look up for env variables
    """
    value = loader.construct_scalar(node)
    return os.environ[value]

# use !env VARIABLENAME to refer env variables
yaml.add_constructor(u'!env', env_constructor)


def build_job(p_name, step):
    """
    Create Job given pipeline and step name
    """

    base_paths = [p_name, './']

    # Remove eventual trailing ".py" and split at dots
    ref = re.sub('\.py$', '', step).split('.')
    # return a possible path for each base_path
    paths = [os.path.join(*[base_path]+ref) for base_path in base_paths]

    # try to load from all possible paths
    loaded = False
    for path in paths:
        try:
            spec = importlib.util.spec_from_file_location(p_name, path+'.py')
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            loaded = True
            break
        except FileNotFoundError:
            pass
    if not loaded:
        raise FileNotFoundError(f'Cannot locate module {step} at {paths}')

    # Try first loading a Job from the module,
    # if there's none try with execute function
    try:
        job = getattr(module, step)
    except AttributeError:
        # TODO check again this code
        inner_fn = getattr(module, 'execute')

        def outer_fn(self, *inputs):
            return inner_fn(*inputs)

        # Create new Job subclass
        NewJob = types.new_class(step, bases=(Job))
        NewJob.execute = MethodType(outer_fn, NewJob)
        job = NewJob
    return job


def build_pipeline(p_name, config_file='pipelines.yml'):
    with open(config_file, 'r') as file:
        pipelines_yaml = yaml.safe_load(file)

    if p_name not in pipelines_yaml:
        raise KeyError(f'Invalid pipeline name: {p_name}')

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

    pipelines = {}

    steps = make_dag(pipelines_yaml[p_name]['steps'])
    try:
        step_ordered = graphlib.TopologicalSorter(steps).static_sort()
    except graphlib.CycleError:
        raise graphlib.CycleError(f'Invalid pipeline definition {p_name}: cycle in steps dependencies')

    # for each step get the source and load it
    jobs = [build_job(p_name, step) for step in steps]
    return Pipeline(jobs)
