import argparse
import struct
from pathlib import Path

from debug import print_progress


def main():
    parser = _create_arg_parser()
    args = parser.parse_args()

    total_lines: int = _number_of_lines(args.input)

    log_progress_step: int = total_lines // 100000
    next_progress_log: int = log_progress_step
    print_progress(0, total_lines)

    with args.input.open("r") as in_file, args.output.open("wb+") as out_file:
        lines_written: int = 0
        for line in in_file:
            value = float(line)
            out_file.write(bytearray(struct.pack("!d", value)))
            lines_written += 1

            if lines_written >= next_progress_log:
                print_progress(lines_written, total_lines)
                next_progress_log += log_progress_step

    print()


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


def _number_of_lines(file: Path) -> int:
    number_of_lines: int = 0
    with file.open("r", encoding="utf-8", errors="ignore") as in_file:
        while True:
            block = in_file.read(65536)
            if not block:
                break

            number_of_lines += block.count("\n")

    return number_of_lines


if __name__ == "__main__":
    main()
