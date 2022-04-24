import logging


class ConfigurationError(RuntimeError):
    """
    Exception raised when an invalid configuration file is found
    """

    def __init__(self, config_errors, relevant_field=None):

        if not relevant_field:
            logging.error("Configuration errors: %s", config_errors)
        else:
            logging.error(
                "Configuration errors for %s: %s",
                relevant_field,
                config_errors[relevant_field],
            )
            if len(config_errors) > 1:
                logging.error(
                    "Other errors: %s",
                    {k: v for k, v in config_errors.items() if k != relevant_field},
                )
        super().__init__("Configuration errors")
