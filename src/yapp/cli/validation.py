import logging
from cerberus import Validator
from cerberus.errors import BasicErrorHandler

from yapp import Pipeline


class ErrorHandler(BasicErrorHandler):
    def _format_message(self, field, error):
            msg = self.messages[error.code].format(
                *error.info, constraint=error.constraint, field=field, value=error.value
                )
            document_path = map(str, error.document_path)
            logging.error(f'Configuration error for {"->".join(document_path)}: {msg}')
            return msg


def check_step(field, value, error):
    pass


def check_step(field, value, error):
    pass


def check_obj(field, value, error):
    if type(value) == dict and len(value) > 1:
        error(field, 'probably missing indent for object parameters')
        error(field, f'Invalid object definition for {next(iter(value))}')
    elif type(value) not in [str, dict]:
        error(field, "Should be of type 'str' or 'dict'")
        error(field, f'Invalid object {value}')


def check_expose(field, value, error):
    pass


pipeline_schema = {
    "inputs": {"type": "list", "schema": {
        "allow_unknown": True, "type": ["string", "dict"], "check_with": check_obj
        }},
    "expose": {"type": "list"},
    "steps": {"type": "list"},
    "outputs": {"type": "list"},
    "hooks": {
         "schema": {hook: {"type": "list"} for hook in Pipeline._valid_hooks}
        },
}

def validate(validator, schema, definitions, allow_unknown=False):
    """
    Validate a level of the configuration
    """
    if allow_unknown:
        validator.schema = {}
        validator.allow_unknown = {"type": "dict", "schema": schema}
    else:
        validator.schema = schema
        validator.allow_unknown = False

    validator.validate(definitions)


def schema_validation(definitions):
    """
    Validate schema for definitions from YAML file
    """

    v = Validator(error_handler=ErrorHandler)

    validate(v, pipeline_schema, definitions, allow_unknown=True)

    return v.errors
