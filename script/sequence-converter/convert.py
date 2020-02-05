import argparse
import struct
from pathlib import Path


def main(arguments):
    with arguments.input.open("r") as in_file, arguments.output.open("wb+") as out_file:
        for line in in_file:
            value = float(line)
            out_file.write(bytearray(struct.pack("!d", value)))


if __name__ == "__main__":
    argument_parser = argparse.ArgumentParser()

    argument_parser.add_argument(
        "--input",
        type=Path,
        required=True
    )

    argument_parser.add_argument(
        "--output",
        type=Path,
        required=True
    )

    arguments = argument_parser.parse_args()
    main(arguments)
