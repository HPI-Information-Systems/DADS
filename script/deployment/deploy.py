import argparse
import getpass
import logging
from pathlib import Path

from worker import deployment_worker


def main():
    logging.basicConfig(format="%(asctime)s : %(levelname)s : %(message)s", level=logging.INFO)

    parser = _create_parser()
    args = parser.parse_args()

    ssh_password: str = getpass.getpass(prompt=f"Enter your SSH password: ")

    deployment_worker.run(args.skip, args.experiment, args.config, ssh_password)

    logging.info("Done")


def _create_parser():
    parser = argparse.ArgumentParser(description=f"Deployment script for distributed java applications.")

    parser.add_argument(
        "--config",
        type=Path,
        required=True,
        help=f"Path to configuration file.",
    )

    parser.add_argument(
        "--skip",
        default=[],
        nargs="+"
    )

    parser.add_argument(
        "--experiment",
        type=str,
        required=False,
        default=None
    )

    return parser


if __name__ == "__main__":
    main()
