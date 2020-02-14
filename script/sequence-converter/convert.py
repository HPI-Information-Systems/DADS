import argparse
import struct
from pathlib import Path


def main():
    parser = _create_arg_parser()
    args = parser.parse_args()

    with args.input.open("r") as in_file, args.output.open("wb+") as out_file:
        for line in in_file:
            value = float(line)
            out_file.write(bytearray(struct.pack("!d", value)))


def _create_arg_parser():
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

    return argument_parser


if __name__ == "__main__":
    main()
