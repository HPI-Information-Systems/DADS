import argparse
import re
import struct
from pathlib import Path

node_pattern = re.compile(r"{(?P<SegmentIndex>\d+)_(?P<NodeIndex>\d+)} (?P<Distance>\d+\.\d+(?:E-?\d+)?)")


def main(arguments):
    
    if arguments.binary:
        max_diff, avg_diff = diff_binary(args.a, args.b, args.out)

    else:
        max_diff, avg_diff = diff_plain(args.a, args.b, args.out)

    print(f"max_diff\t\t{max_diff}")
    print(f"avg_diff\t\t{avg_diff}")



def diff_plain(a: Path, b: Path, result: Path):
    max_diff = 0.0
    total_diff = 0.0
    compared_values = 0
    with a.open("r") as a_in, b.open("r") as b_in, result.open("w+") as result_out:
        header_line_a = next(a_in)
        header_line_b = next(b_in)

        if not header_line_a == header_line_b:
            print("matrix format does not match!")
            return

        next(a_in)  # skip blank line
        next(b_in)  # skip blank line

        for line_a in a_in:
            line_a = line_a.replace("\n", "")
            line_b = next(b_in).replace("\n", "")

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
                print(diff, file=result_out, end="\t")

            print("", file=result_out)

    return max_diff, total_diff / compared_values


def diff_binary(a: Path, b: Path, result: Path):
    max_diff = 0.0
    total_diff = 0.0
    compared_values = 0
    with a.open("rb") as a_in, b.open("rb") as b_in, result.open("w+") as result_out:
        rows_a = struct.unpack("!q", a_in.read(8))[0]
        columns_a = struct.unpack("!q", a_in.read(8))[0]

        rows_b = struct.unpack("!q", b_in.read(8))[0]
        columns_b = struct.unpack("!q", b_in.read(8))[0]

        if rows_a != rows_b or columns_a != columns_b:
            print("matrix format does not match!")
            return 0.0, 0.0

        for row_index in range(rows_a):
            for columns_index in range(columns_a):
                value_a = struct.unpack("!d", a_in.read(8))[0]
                value_b = struct.unpack("!d", b_in.read(8))[0]

                diff = abs(value_a - value_b)
                total_diff += diff
                compared_values += 1
                if diff > max_diff:
                    max_diff = diff
                print(diff, file=result_out, end="\t")
            
            print("", file=result_out)

    return max_diff, total_diff / compared_values


if __name__ == "__main__":
    argument_parser = argparse.ArgumentParser()

    argument_parser.add_argument(
        "--binary",
        type=bool,
        choices=[True, False],
        required=False,
        default=False,
    )
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
