import argparse
import re
from pathlib import Path

node_pattern = re.compile(r"{(?P<SegmentIndex>\d+)_(?P<NodeIndex>\d+)} (?P<Distance>\d+\.\d+(?:E-?\d+)?)")


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

            match_a = node_pattern.fullmatch(line_a)

            if match_a is None:
                print(f"ill formatted line in a: {line_a}")
                continue

            match_b = node_pattern.fullmatch(line_b)

            if match_b is None:
                print(f"ill formatted line in b: {line_b}")
                continue

            segment_a = int(match_a.group("SegmentIndex"))
            node_a = int(match_a.group("NodeIndex"))
            distance_a = float(match_a.group("Distance"))

            segment_b = int(match_b.group("SegmentIndex"))
            node_b = int(match_b.group("NodeIndex"))
            distance_b = float(match_b.group("Distance"))

            if segment_a != segment_b or node_a != node_b:
                print(f"node mismatch! ({line_a} vs. {line_b})")
                continue

            diff = abs(distance_a - distance_b)
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
