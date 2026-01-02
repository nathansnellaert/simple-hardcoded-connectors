import argparse

from subsets_utils import validate_environment
from ingest import data_sources as ingest_data
from transforms.big_mac_index import main as transform_big_mac
from transforms.freddie_mac import main as transform_freddie_mac
from transforms.pmms import main as transform_pmms


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--ingest-only", action="store_true", help="Only fetch data from API")
    parser.add_argument("--transform-only", action="store_true", help="Only transform existing raw data")
    args = parser.parse_args()

    validate_environment()

    should_ingest = not args.transform_only
    should_transform = not args.ingest_only

    if should_ingest:
        print("\n=== Phase 1: Ingest ===")
        ingest_data.run()

    if should_transform:
        print("\n=== Phase 2: Transform ===")
        print("\n--- Big Mac Index ---")
        transform_big_mac.run()
        print("\n--- Freddie Mac ---")
        transform_freddie_mac.run()
        print("\n--- PMMS Mortgage Rates ---")
        transform_pmms.run()


if __name__ == "__main__":
    main()
