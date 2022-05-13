import logging
import re

from cerberus import Validator
from cerberus.errors import BasicErrorHandler
from cerberus import schema_registry

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
    # Checkin manually gives more control on error messages and more flexibility on
    # types handling. In this case it could have been done entirely with cerberus
    # "type": "string", 'regex': r'^[a-zA-Z_.][a-zA-Z0-9_.]*[\.[a-zA-Z_.]+[a-zA-Z0-9_.]*]*$'

    # Very very compact, (redundant) type check and regex
    if not isinstance(value, str) or (
        not re.match(r"^[a-zA-Z_.][a-zA-Z0-9_.]*[\.[a-zA-Z_.]+[a-zA-Z0-9_.]*]*$", value)
    ):
        error(field, f"{value} is not a valid reference string")


schema_registry.add(
    "expose",
    {
        "use": {"required": True, "type": "string"},
        "as": {"required": True, "type": ["string", "list"]},
    },
)

schema_registry.add(
    "hook",
    {
        "run": {"required": True, "type": "string", "check_with": check_code_reference},
        "on": {
            "required": True,
            "type": ["string", "list"],
            "allowed": Pipeline.VALID_HOOKS,
        },
    },
)

schema_registry.add(
    "step",
    {
        "run": {"required": True, "type": "string"},
        "after": {"required": False, "type": ["string", "list"]},
        "with": {"required": False, "type": "dict"},
    },
)

# Overall pipelines.yml schema
pipeline_schema = {
    "inputs": {
        "required": False,
        "type": "list",
        "schema": {
            "allow_unknown": False,
            "type": "dict",
            "schema": {
                "from": {"required": True, "type": "string"},
                "with": {"required": False, "type": "dict"},
                "expose": {
                    "required": False,
                    "type": "list",
                    "schema": {
                        "type": "dict",
                        "schema": "expose",
                    },
                },
            },
        },
    },
    "steps": {
        "required": True,
        "type": "list",
        "schema": {
            "type": "dict",
            "schema": "step",
        },
    },
    "outputs": {
        "required": False,
        "type": "list",
        "schema": {
            "allow_unknown": False,
            "type": "dict",
            "schema": {
                "to": {"required": True, "type": "string"},
                "with": {"required": False, "type": "dict"},
                # "save": {"type": "to_save"},
            },
        },
    },
    "hooks": {
        "required": False,
        "type": "list",
        "schema": {
            "type": "dict",
            "schema": "hook",
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
