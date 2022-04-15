from yapp import Pipeline
from .simple_pipeline import PreProcessor, Predictor, PostProcessor


class ExamplePipeline(Pipeline):
    job_list = [PreProcessor, Predictor, PostProcessor]

