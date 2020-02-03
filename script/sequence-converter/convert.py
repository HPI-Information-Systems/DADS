import argparse
import struct
from pathlib import Path


def main(arguments):
    with arguments.input.open("r") as input, open("output.bin", "wb+") as output:
        for line in input:
            value = float(line)
            output.write(bytearray(struct.pack("!d", value)))


if __name__ == "__main__":
    argument_parser = argparse.ArgumentParser()

    argument_parser.add_argument(
        "--input",
        type=Path,
        required=True
    )

    arguments = argument_parser.parse_args()
    main(arguments)
