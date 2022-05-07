class Monitor:
    """Pipeline status monitoring class
    Wrapper class used to group hooks"""

    def __init__(self, pipeline):
        self.pipeline = pipeline
