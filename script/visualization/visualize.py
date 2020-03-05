import argparse
from pathlib import Path
from typing import List, Optional, Dict

from event import *
from plotter import Plotter


def main():
    parser = _create_arg_parser()
    args = parser.parse_args()

    events: Dict[str, Dict[str, List[base.StatisticsEvent]]] = _load_events(args.input, args.input_filter)

    for processor in sorted(events):
        plotter: Plotter = Plotter(processor, events[processor])
        plotter.show()


def _create_arg_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input",
        type=Path,
        required=True,
        help=f"Path to the statistics folder",
    )

    parser.add_argument(
        "--input-filter",
        type=str,
        required=False,
        help=f"Filter for statistics files",
        default="*.log",
    )

    return parser


def _load_events(input_dir: Path, input_filter: str) -> Dict[str, Dict[str, List[base.StatisticsEvent]]]:
    events: Dict[str, Dict[str, List[base.StatisticsEvent]]] = {}
    number_of_events: int = 0

    for file in input_dir.glob(input_filter):
        with file.open("r") as in_file:
            for line in in_file:
                event: Optional[base.StatisticsEvent] = base.StatisticsEvent.from_log_ling(line)
                if not event:
                    continue

                processor: str = event.processor
                event_type: str = event.event_identifier()

                if processor not in events:
                    events[processor] = {}

                if event_type not in events[processor]:
                    events[processor][event_type] = []

                events[processor][event_type].append(event)
                number_of_events += 1

    print(f"Extracted {number_of_events} events")
    return events


if __name__ == "__main__":
    main()
