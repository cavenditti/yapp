import logging
import sys

enable_color = False


def addLoggingLevel(levelName, levelNum, methodName=None):
    """
    Comprehensively adds a new logging level to the `logging` module and the
    currently configured logging class.

    `levelName` becomes an attribute of the `logging` module with the value
    `levelNum`. `methodName` becomes a convenience method for both `logging`
    itself and the class returned by `logging.getLoggerClass()` (usually just
    `logging.Logger`). If `methodName` is not specified, `levelName.lower()` is
    used.

    To avoid accidental clobberings of existing attributes, this method will
    raise an `AttributeError` if the level name is already an attribute of the
    `logging` module or if the method name is already present

    Example
    -------
    >>> addLoggingLevel('TRACE', logging.DEBUG - 5)
    >>> logging.getLogger(__name__).setLevel("TRACE")
    >>> logging.getLogger(__name__).trace('that worked')
    >>> logging.trace('so did this')
    >>> logging.TRACE
    5

    """
    if not methodName:
        methodName = levelName.lower()

    if hasattr(logging, levelName):
        raise AttributeError("{} already defined in logging module".format(levelName))
    if hasattr(logging, methodName):
        raise AttributeError("{} already defined in logging module".format(methodName))
    if hasattr(logging.getLoggerClass(), methodName):
        raise AttributeError("{} already defined in logger class".format(methodName))

    # This method was inspired by the answers to Stack Overflow post
    # http://stackoverflow.com/q/2183233/2988730, especially
    # http://stackoverflow.com/a/13638084/2988730
    def logForLevel(self, message, *args, **kwargs):
        if self.isEnabledFor(levelNum):
            self._log(levelNum, message, args, **kwargs)

    def logToRoot(message, *args, **kwargs):
        logging.log(levelNum, message, *args, **kwargs)

    logging.addLevelName(levelNum, levelName)
    setattr(logging, levelName, levelNum)
    setattr(logging.getLoggerClass(), methodName, logForLevel)
    setattr(logging, methodName, logToRoot)


def get_color(loglevel=None):
    """
    Returns the color escape characters to print
    """
    if not enable_color:
        return ""

    white = "\x1b[1;37m"
    gray = "\x1b[38;5;247m"
    yellow = "\x1b[1;33m"
    blue = "\x1b[1;34m"
    # purple = "\x1b[1;35m"
    green = "\x1b[1;32m"
    red = "\x1b[31;1m"
    reset = "\x1b[0m"

    FORMATS = {
        logging.DEBUG: white,
        logging.INFO: blue,
        logging.OK: green,
        logging.PRINT: gray,
        logging.WARNING: yellow,
        logging.ERROR: red,
        logging.CRITICAL: red,
    }
    return FORMATS.get(loglevel, reset)


class LogFormatter(logging.Formatter):
    width = 26

    def format(self, record):
        # An ugly hack to prevent printing empty lines from print calls (1/3)
        if not str(record.msg).strip().strip("\n"):
            return ""
        if len(record.module) > 10:
            record.module = record.module[:10] + "…"
        if len(record.funcName) > 10:
            record.funcName = record.funcName[:10] + "…"
        levelname = record.levelname[:1]
        lineno = str(record.lineno)
        head = "%s %s.%s" % (levelname[:1], record.module, record.funcName)
        head = "[" + head.ljust(self.width - len(lineno)) + " " + lineno + "]"
        record.msg = str(record.msg)
        if record.msg.startswith("> "):
            head = get_color(record.levelno) + head
            record.msg = get_color(logging.DEBUG) + record.msg[2:] + get_color()
        else:
            head = get_color(record.levelno) + head + get_color()

        # An ugly hack to prevent printing empty lines from print calls (2/3)
        return "%s %s\n" % (head, record.msg.lstrip())


def setup_logging(loglevel, color=False):
    # setup colored output
    # TODO use an attribute instead of a global variable
    global enable_color
    enable_color = color

    logger = logging.getLogger()

    addLoggingLevel("OK", 21)
    addLoggingLevel("PRINT", 22)

    logger.setLevel(logging.DEBUG)
    ch = logging.StreamHandler()
    # An ugly hack to prevent printing empty lines from print calls (3/3)
    ch.terminator = ""
    ch.setLevel(getattr(logging, loglevel))
    formatter = LogFormatter()
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    # send print calls from Jobs and Hooks to log.
    # Even though there are probably better ways of doing this,
    # I prefer this one because keeps the track of where print is called
    # the downside is that the prints are messed up and splitted
    sys.stdout.write = logger.print
