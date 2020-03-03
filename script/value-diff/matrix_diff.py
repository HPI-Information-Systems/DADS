import argparse
import re
from pathlib import Path

node_pattern = re.compile(r"{(?P<SegmentIndex>\d+)_(?P<NodeIndex>\d+)} (?P<Distance>\d+\.\d+(?:E-?\d+)?)")


def main(arguments):
    max_diff = 0.0
    total_diff = 0.0
    compared_values = 0
    with arguments.a.open("r") as a, arguments.b.open("r") as b, arguments.out.open("w+") as out_file:
        header_line_a = next(a)
        header_line_b = next(b)

        if not header_line_a == header_line_b:
            print("matrix format does not match!")
            return

        next(a)  # skip blank line
        next(b)  # skip blank line

        for line_a in a:
            line_a = line_a.replace("\n", "")
            line_b = next(b).replace("\n", "")

            values_a = [float(x) for x in line_a.split("\t") if x]
            values_b = [float(x) for x in line_b.split("\t") if x]

            if len(values_a) != len(values_b):
                print("number of columns does not match!")
                return

            for i in range(len(values_a)):
                diff = abs(values_a[i] - values_b[i])
                total_diff += diff
                compared_values += 1
                if diff > max_diff:
                    max_diff = diff
                print(diff, file=out_file, end="\t")

            print("", file=out_file)

    print(f"max_diff = {max_diff}")
    print(f"avg_diff = {total_diff / compared_values}")


if __name__ == "__main__":
    argument_parser = argparse.ArgumentParser()

    argument_parser.add_argument(
        "-a",
        type=Path,
        required=True
    )
    argument_parser.add_argument(
        "-b",
        type=Path,
        required=True
    )
    argument_parser.add_argument(
        "--out",
        type=Path,
        required=True
    )

    args = argument_parser.parse_args()
    main(args)
