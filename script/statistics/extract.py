import argparse
import re
from datetime import datetime
from pathlib import Path
from typing import List, Dict

from log_events import UtilizationEvent

LOG_DATE_TIME = "LogDateTime"
LOG_EVENT_NAME = "LogEvent"
LOG_EVENT_PROPERTIES = "LogEventProperties"
log_line_regex = re.compile(
    r"\[(?P<LogDateTime>\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}\.\d{3})]\s{2}-\s{2}(?P<LogEvent>\w+) { (?P<LogEventProperties>.+? )}")


def main():
    parser = _create_arg_parser()
    args = parser.parse_args()

    args.output.mkdir(parents=True, exist_ok=True)

    utilization_events: Dict[str, List[UtilizationEvent]] = {}

    with args.input.open("r") as in_file:
        for line in in_file:
            match = log_line_regex.match(line)
            if not match:
                continue

            if match.group(LOG_EVENT_NAME) == UtilizationEvent.event_name():
                event = UtilizationEvent.parse(match.group(LOG_EVENT_PROPERTIES))

                if event.processor not in utilization_events:
                    utilization_events[event.processor] = []

                utilization_events[event.processor].append(event)
                continue

    _save_utilization_events(utilization_events, args.output)


def _create_arg_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input",
        type=Path,
        required=True,
        help=f"Statistics file to extract data from"
    )

    parser.add_argument(
        "--output",
        type=Path,
        required=True,
        help=f"Path to folder to extract data to"
    )

    return parser


def _save_utilization_events(events: Dict[str, List[UtilizationEvent]], out_dir: Path) -> None:
    for processor in events:
        sorted_events: List[UtilizationEvent] = sorted(events[processor], key=lambda x: x.date_time)
        if not sorted_events:
            continue

        first_date_time: datetime = sorted_events[0].date_time
        with Path(out_dir, f"utilization-{processor}.csv").open("w+") as out_file:
            print(f"time; used_memory; used_cpu", file=out_file)

            for event in sorted_events:
                print(f"{event.date_time - first_date_time}; {event.used_memory}; {event.used_cpu}", file=out_file)


if __name__ == "__main__":
    main()
