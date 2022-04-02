from yapp import Pipeline
from .simple_pipeline import PreProcessor, Predictor, Processor

class ExamplePipeline(Pipeline):
    def run(self):
        self.run_job(PreProcessor)
        self.run_job(Predictor)
        self.run_job(PostProcessor)

