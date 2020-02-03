import argparse
from pathlib import Path


def main(arguments):
    max_diff = 0.0
    total_diff = 0.0
    compared_values = 0
    with arguments.a.open("r") as a, arguments.b.open("r") as b, arguments.out.open("w+") as out_file:
        for line_a in a:
            line_a = line_a.replace("\n", "")
            line_b = b.readline().replace("\n", "")

            if len(line_a) < 1 and len(line_b) < 1:
                continue
            elif len(line_a) < 1:
                print("missing line in a!")
                continue
            elif len(line_b) < 1:
                print("missing line in b!")
                continue

            value_a = float(line_a)
            value_b = float(line_b)
            diff = abs(value_a - value_b)
            total_diff += diff
            compared_values += 1
            if diff > max_diff:
                max_diff = diff
            print(diff, file=out_file)

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