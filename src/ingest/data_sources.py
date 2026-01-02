
"""Ingest data from various simple data sources."""

from subsets_utils import get, save_raw_json

DATA_SOURCES = {
    "big_mac_index": "https://raw.githubusercontent.com/TheEconomist/big-mac-data/master/output-data/big-mac-full-index.csv",
    "freddie_mac": "https://www.freddiemac.com/fmac-resources/research/docs/fmhpi_master_file.csv",
    "pmms": "https://www.freddiemac.com/pmms/docs/PMMS_history.csv",
}


def run():
    """Fetch all data sources."""
    all_data = {}

    for name, url in DATA_SOURCES.items():
        print(f"  Fetching {name}...")
        response = get(url, timeout=300.0)
        response.raise_for_status()
        all_data[name] = response.text
        print(f"    Downloaded {len(response.text)} bytes")

    save_raw_json(all_data, "data_sources", compress=True)
