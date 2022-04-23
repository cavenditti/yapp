from .core import InputAdapter, Inputs, Job, OutputAdapter, Pipeline

__all__ = [
    "core",
    "adapters",
    "cli",
    "Pipeline",
    "Job",
    "Inputs",
    "InputAdapter",
    "OutputAdapter",
]


# FIXME move somewhere else
class ConfigurationError(RuntimeError):
    def __init__(self, config_errors, relevant_field=None):
        import logging

        if not relevant_field:
            logging.error(f"Configuration errors: {config_errors}")
        else:
            logging.error(
                f"Configuration errors for {relevant_field}: {config_errors[relevant_field]}"
            )
            if len(config_errors) > 1:
                logging.error(
                    f"Other errors: { {k:v for k,v in config_errors.items() if k != relevant_field} }"
                )
        super().__init__("Configuration errors")
