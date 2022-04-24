"""
yapp cli parsing
"""

import argparse
import inspect
import logging
import sys

from yapp.cli.logs import setup_logging
from yapp.cli.parsing import create_pipeline


def main():
    """
    yapp cli entrypoint
    """

    parser = argparse.ArgumentParser(description="Run yapp pipeline")

    parser.add_argument(
        "-p",
        "--path",
        nargs="?",
        default="./",
        help="Path to look in for pipelines definitions",
    )

    parser.add_argument(
        "-d",
        "--debug",
        action="store_const",
        dest="loglevel",
        const="DEBUG",
        default="INFO",
        help="Set loglevel to DEBUG, same as --loglevel=DEBUG",
    )

    parser.add_argument(
        "-l",
        "--loglevel",
        nargs="?",
        dest="loglevel",
        default="INFO",
        help="Log level to use",
    )

    parser.add_argument(
        "-f",
        "--logfile",
        nargs="?",
        dest="logfile",
        type=str,
        default="",
        help="Log level to use",
    )

    parser.add_argument(
        "--color",
        action="store_const",
        dest="color",
        const=True,
        default=False,
        help="Print colored output for logs",
    )

    parser.add_argument("pipeline", type=str, help="Pipeline name")

    args = parser.parse_args()
    loglevel = args.loglevel.upper()

    setup_logging(loglevel, color=args.color, logfile=args.logfile)

    # Read configuration and create a new pipeline
    try:
        pipeline = create_pipeline(args.pipeline, path=args.path)
    except Exception as error:   # pylint: disable=broad-except
        logging.exception(error)
        sys.exit(-1)

    # Run the pipeline
    try:
        pipeline()
    except Exception as error:   # pylint: disable=broad-except

        logging.exception(error)
        logging.debug("pipeline.inputs: %s", pipeline.inputs.__repr__())
        logging.debug("pipeline.outputs: %s", pipeline.outputs)
        logging.debug("pipeline.job_list: %s", pipeline.job_list)
        for job in pipeline.job_list:
            args = inspect.getfullargspec(job.execute).args
            logging.debug("%s.execute arguments: %s", job, args)
        sys.exit(-1)


if __name__ == "__main__":
    main()
