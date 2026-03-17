from __future__ import annotations

import argparse


def main() -> None:
    parser = argparse.ArgumentParser(description="tgarefill helper CLI")
    parser.add_argument(
        "message",
        nargs="?",
        default="Use the scripts/ directory for the main pipeline.",
        help="Optional message to print.",
    )
    args = parser.parse_args()
    print(args.message)


if __name__ == "__main__":
    main()
