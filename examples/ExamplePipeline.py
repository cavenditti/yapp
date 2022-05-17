from yapp import Pipeline

from .simple_pipeline import PostProcessor, Predictor, PreProcessor


class ExamplePipeline(Pipeline):
    job_list = [PreProcessor, Predictor, PostProcessor]
