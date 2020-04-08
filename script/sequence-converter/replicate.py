import argparse
from pathlib import Path
from typing import List

from debug import print_progress


def main():
    parser = _create_arg_parser()
    args = parser.parse_args()

    desired_number_of_lines: int = int(args.lines.replace("K", "000").replace("M", "000000").replace("B", "000000000"))

    log_progress_step: int = desired_number_of_lines // 100000
    next_progress_log: int = log_progress_step
    print_progress(0, desired_number_of_lines)

    with args.output.open("w+") as out_file:
        lines_written: int = 0
        lines: List[str] = []
        with args.input.open("r") as in_file:
            for line in in_file:
                if lines_written >= desired_number_of_lines:
                    break

                lines.append(line)
                out_file.write(line)
                lines_written += 1

                if lines_written >= next_progress_log:
                    print_progress(lines_written, desired_number_of_lines)
                    next_progress_log += log_progress_step

        while lines_written < desired_number_of_lines:
            for line in lines:
                if lines_written >= desired_number_of_lines:
                    break
                out_file.write(line)
                lines_written += 1

                if lines_written >= next_progress_log:
                    print_progress(lines_written, desired_number_of_lines)
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
        "--lines",
        type=str,
        required=True,
    )

    argument_parser.add_argument(
        "--output",
        type=Path,
        required=True
    )

    return argument_parser


if __name__ == "__main__":
    main()
