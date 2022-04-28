import logging
import re

from cerberus import Validator
from cerberus.errors import BasicErrorHandler

from yapp import Pipeline


class ErrorHandler(BasicErrorHandler):  # pylint: disable=abstract-method
    """
    Cerberus custom ErrorHandler to print config errors the way I want
    """

    def _format_message(self, field, error):
        msg = self.messages[error.code].format(
            *error.info, constraint=error.constraint, field=field, value=error.value
        )
        document_path = map(str, error.document_path)
        logging.error("%s: %s", "->".join(document_path), msg)
        return msg


def check_code_reference(field, value, error):
    """
    Check if a string can be a valid python module or function reference
    """
    # Very very compact, tpye check and regex
    if not isinstance(value, str) or (
        not re.match(r"^[a-zA-Z_.][a-zA-Z0-9_.]*[\.[a-zA-Z_.]+[a-zA-Z0-9_.]*]*$", value)
    ):
        error(field, f"{value} is not a valid reference string")


def check_step(field, value, error):
    """
    Ugly "step" definition checker
    """
    if isinstance(value, dict):
        if len(value) != 1:
            error(field, f"Invalid step definition for {next(iter(value))}")
            error(field, "Steps must have 1 single value: the step dependencies")
        deps = next(iter(value.values()))
        if isinstance(deps, list):
            for dep in deps:
                check_code_reference(field, dep, error)
        elif isinstance(deps, str):
            check_code_reference(field, deps, error)
    elif isinstance(value, str):
        check_code_reference(field, value, error)
    else:
        error(field, "Should be of type 'str' or 'dict'")
        error(field, f"Invalid step definition {value}")


def check_adapter(field, value, error):
    """
    Ugly "adapter" definition checker
    """
    if isinstance(value, dict) and len(value) > 1:
        error(field, "Probably missing indent for object parameters")
        error(field, f"Invalid adapter definition for {next(iter(value))}")
    elif not isinstance(value, (str, dict)):
        error(field, "Should be of type 'str' or 'dict'")
        error(field, f"Invalid adapter definition {value}")


def check_expose(field, value, error):
    """
    Ugly "expose" definition checker
    """
    if len(value) != 1:
        error(field, f"Invalid expose definition for {next(iter(value))}")
    expose_list = next(iter(value.values()))
    if not isinstance(expose_list, list):
        error(field, "Expecting list of exposed values")
    for expose_dict in expose_list:
        if len(expose_dict) != 1:
            error(field, f"Too many fields in expose for {next(iter(expose_dict))}")
        source, exposed = next(iter(expose_dict.items()))
        if not isinstance(source, str):
            error(field, f"Expecting string, not {type(source)}")
        if not isinstance(exposed, str):
            error(field, f"Expecting string, not {type(exposed)}")


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
            for hook in Pipeline.valid_hooks
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
