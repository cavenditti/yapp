class BasicMonitor:
    def __init__(self, yup):
        self.yup = yup

    def job_start(self, pipeline):
        print(f"I'm printing from a monitor with {self.yup}")
