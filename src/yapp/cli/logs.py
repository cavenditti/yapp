import logging
import sys
import traceback
from io import StringIO

from yapp import Pipeline

OK = 24
PRINT = 22


def add_logging_level(level_name, level_num, method_name=None):
    """
    Comprehensively adds a new logging level to the `logging` module and the
    currently configured logging class.

    `level_name` becomes an attribute of the `logging` module with the value
    `level_num`. `method_name` becomes a convenience method for both `logging`
    itself and the class returned by `logging.getLoggerClass()` (usually just
    `logging.Logger`). If `method_name` is not specified, `level_name.lower()` is
    used.

    To avoid accidental clobberings of existing attributes, this method will
    raise an `AttributeError` if the level name is already an attribute of the
    `logging` module or if the method name is already present

    Example
    -------
    >>> add_logging_level('TRACE', logging.DEBUG - 5)
    >>> logging.getLogger(__name__).setLevel("TRACE")
    >>> logging.getLogger(__name__).trace('that worked')
    >>> logging.trace('so did this')
    >>> logging.TRACE
    5

    """
    if not method_name:
        method_name = level_name.lower()

    if hasattr(logging, level_name):
        raise AttributeError(f"{level_name} already defined in logging module")
    if hasattr(logging, method_name):
        raise AttributeError(f"{method_name} already defined in logging module")
    if hasattr(logging.getLoggerClass(), method_name):
        raise AttributeError(f"{method_name} already defined in logger class")

    # This method was inspired by the answers to Stack Overflow post
    # http://stackoverflow.com/q/2183233/2988730, especially
    # http://stackoverflow.com/a/13638084/2988730
    def log_for_level(self, message, *args, **kwargs):
        if self.isEnabledFor(level_num):
            self._log(  # pylint: disable=protected-access
                level_num, message, args, **kwargs
            )

    def log_to_root(message, *args, **kwargs):
        logging.log(level_num, message, *args, **kwargs)

    logging.addLevelName(level_num, level_name)
    setattr(logging, level_name, level_num)
    setattr(logging.getLoggerClass(), method_name, log_for_level)
    setattr(logging, method_name, log_to_root)


class LogFormatter(logging.Formatter):
    """
    Custom LogFormatter, probably not the best way at all to do this but was fun doing it this way.
    """

    def __init__(self, width=26, color=False, show_lineno=False):
        self.width = width
        self.color = color
        self.show_lineno = show_lineno
        super().__init__()

    def get_color(self, loglevel=None):
        """
        Returns the color escape characters to print
        """
        if not self.color:
            return ""

        white = "\x1b[1;37m"
        gray = "\x1b[38;5;247m"
        yellow = "\x1b[1;33m"
        blue = "\x1b[1;34m"
        # purple = "\x1b[1;35m"
        green = "\x1b[1;32m"
        red = "\x1b[31;1m"
        reset = "\x1b[0m"

        formats = {
            logging.DEBUG: white,
            logging.INFO: blue,
            logging.OK: green,  # pylint: disable=no-member
            logging.PRINT: gray,  # pylint: disable=no-member
            logging.WARNING: yellow,
            logging.ERROR: red,
            logging.CRITICAL: red,
        }
        return formats.get(loglevel, reset)

    def formatException(self, exc_info):
        with StringIO() as tb_str_io:
            traceback.print_tb(exc_info[2], file=tb_str_io)
            tb_str = tb_str_io.getvalue()
        return (
            exc_info[0].__name__
            + ": "
            + exc_info[1].args[0]
            + "\n"
            + self.get_color()
            + tb_str
        )

    def format(self, record):
        # An ugly hack to prevent printing empty lines from print calls (1/3)
        if not str(record.msg).strip().strip("\n"):
            return ""
        if len(record.module) > 10:
            record.module = record.module[:10] + "…"
        if len(record.funcName) > 10:
            record.funcName = record.funcName[:10] + "…"
        levelname = record.levelname[:1]
        head = f"{levelname[:1]} {record.module}.{record.funcName}"
        if self.show_lineno:
            lineno = str(record.lineno)
            head = "[" + head.ljust(self.width - len(lineno)) + " " + lineno + "]"
        else:
            head = "[" + head.ljust(self.width) + "]"
        msg = str(record.msg) % record.args

        if record.exc_info:
            msg = "> " + self.formatException(record.exc_info)

        if msg.startswith("> "):
            head = self.get_color(record.levelno) + head
            # msg = self.get_color(logging.DEBUG) + msg[2:] + self.get_color()
            msg = msg[2:] + self.get_color()
        else:
            head = self.get_color(record.levelno) + head + self.get_color()

        # An ugly hack to prevent printing empty lines from print calls (2/3)
        return f"{head} {msg.lstrip()}\n"


def setup_logging(loglevel, color=False, logfile="", show_lineno=False):
    """
    Setup logging for yapp
    """

    logger = logging.getLogger()

    add_logging_level("OK", OK)
    add_logging_level("PRINT", PRINT)

    logger.setLevel(logging.DEBUG)

    handlers = [(logging.StreamHandler(), color)]
    if logfile:
        handlers.append((logging.FileHandler(logfile), False))

    for pair in handlers:
        handler, color = pair
        # An ugly hack to prevent printing empty lines from print calls (3/3)
        handler.terminator = ""
        handler.setLevel(getattr(logging, loglevel))
        formatter = LogFormatter(color=color, show_lineno=show_lineno)
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    # Use custom loglevel for Pipeline success messages
    Pipeline.OK_LOGLEVEL = OK

    # send print calls from Jobs and Hooks to log.
    # Even though there are probably better ways of doing this,
    # I prefer this one because keeps the track of where print is called
    # the downside is that the prints are messed up and splitted
    sys.stdout.write = logger.print
