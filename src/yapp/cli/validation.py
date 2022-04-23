import logging
import re

from cerberus import Validator
from cerberus.errors import BasicErrorHandler

from yapp import Pipeline


class ErrorHandler(BasicErrorHandler):
    def _format_message(self, field, error):
        msg = self.messages[error.code].format(
            *error.info, constraint=error.constraint, field=field, value=error.value
        )
        document_path = map(str, error.document_path)
        logging.error(f'{"->".join(document_path)}: {msg}')
        return msg


def check_code_reference(field, value, error):
    """
    Check if a string can be a valid python module or function reference
    """
    # Very very compact, tpye check and regex
    if type(value) is not str or (
        not re.match(r"^[a-zA-Z_.][a-zA-Z0-9_.]*[\.[a-zA-Z_.]+[a-zA-Z0-9_.]*]*$", value)
    ):
        error(field, f"{value} is not a valid reference string")


def check_step(field, value, error):
    """
    Ugly "step" definition checker
    """
    if type(value) is dict:
        if len(value) != 1:
            error(field, f"Invalid step definition for {next(iter(value))}")
            error(field, "Steps must have 1 single value: the step dependencies")
        deps = next(iter(value.values()))
        if type(deps) is list:
            for dep in deps:
                check_code_reference(field, dep, error)
        elif type(deps) is str:
            check_code_reference(field, deps, error)
    elif type(value) is str:
        check_code_reference(field, value, error)
    else:
        error(field, "Should be of type 'str' or 'dict'")
        error(field, f"Invalid step definition {value}")


def check_adapter(field, value, error):
    """
    Ugly "adapter" definition checker
    """
    if type(value) is dict and len(value) > 1:
        error(field, "Probably missing indent for object parameters")
        error(field, f"Invalid adapter definition for {next(iter(value))}")
    elif type(value) not in [str, dict]:
        error(field, "Should be of type 'str' or 'dict'")
        error(field, f"Invalid adapter definition {value}")


def check_expose(field, value, error):
    """
    Ugly "expose" definition checker
    """
    if len(value) != 1:
        error(field, f"Invalid expose definition for {next(iter(value))}")
    expose_list = next(iter(value.values()))
    if type(expose_list) is not list:
        error(field, f"Expecting list of exposed values")
    for expose_dict in expose_list:
        if len(expose_dict) is not 1:
            error(field, f"Too many fields in expose for {next(iter(expose_dict))}")
        a, b = next(iter(expose_dict.items()))
        if type(a) is not str:
            error(field, f"Expecting string, not {a}")
        if type(b) is not str:
            error(field, f"Expecting string, not {b}")


# Overall pipelines.yml schema
pipeline_schema = {
    "inputs": {
        "type": "list",
        "schema": {
            "allow_unknown": True,
            "type": ["string", "dict"],
            "check_with": check_adapter,
        },
    },
    "expose": {"type": "list", "schema": {"type": "dict", "check_with": check_expose}},
    "steps": {
        "type": "list",
        "schema": {
            "type": ["dict", "string"],
            "check_with": check_step,
        },
    },
    "outputs": {
        "type": "list",
        "schema": {
            "allow_unknown": True,
            "type": ["string", "dict"],
            "check_with": check_adapter,
        },
    },
    "hooks": {
        "schema": {
            hook: {
                "type": "list",
                "schema": {
                    # Checkin manually gives more control on error messages and more flexibility on
                    # types handling. In this case it could have been done entirely with cerberus
                    # "type": "string", 'regex': r'^[a-zA-Z_.][a-zA-Z0-9_.]*[\.[a-zA-Z_.]+[a-zA-Z0-9_.]*]*$'
                    "check_with": check_code_reference
                },
            }
            for hook in Pipeline._valid_hooks
        },
    },
}


def validate(definitions):
    """
    Validate schema for definitions from YAML file
    """

    validator = Validator(error_handler=ErrorHandler)
    validator.schema = {}
    validator.allow_unknown = {"type": "dict", "schema": pipeline_schema}
    validator.validate(definitions)

    return validator.errors
