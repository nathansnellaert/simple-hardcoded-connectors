"""Transform Freddie Mac Primary Mortgage Market Survey data."""

import pyarrow as pa
import pyarrow.csv as csv
import pyarrow.compute as pc
from subsets_utils import load_raw_json, upload_data, publish
from .test import test

DATASET_ID = "freddie_mac_mortgage_rates"

METADATA = {
    "id": DATASET_ID,
    "title": "Freddie Mac Mortgage Rates (PMMS)",
    "description": "Weekly average mortgage rates from Freddie Mac's Primary Mortgage Market Survey, the most-cited source of US mortgage rates. Data from 1971 to present.",
    "column_descriptions": {
        "date": "Survey date (YYYY-MM-DD)",
        "rate_30yr": "30-year fixed-rate mortgage average (%)",
        "points_30yr": "30-year fixed-rate points/fees (%)",
        "rate_15yr": "15-year fixed-rate mortgage average (%)",
        "points_15yr": "15-year fixed-rate points/fees (%)",
    }
}


def run():
    """Transform PMMS mortgage rate data."""
    raw_data = load_raw_json("data_sources")
    csv_text = raw_data["pmms"]

    # Parse CSV - read all columns as strings to handle varying column counts
    # After Nov 2022, ARM columns were dropped (9 cols -> 4 cols)
    lines = csv_text.strip().split("\n")
    header = lines[0]

    # Normalize all rows to have same columns as header
    rows = []
    for line in lines[1:]:
        values = line.split(",")
        # Pad with empty values if needed
        while len(values) < 9:
            values.append("")
        rows.append(values[:9])

    # Build table manually
    dates = [r[0] for r in rows]
    pmms30 = [r[1] for r in rows]
    pmms30p = [r[2] for r in rows]
    pmms15 = [r[3] for r in rows]
    pmms15p = [r[4] for r in rows]

    table = pa.table({
        "date": pa.array(dates, type=pa.string()),
        "pmms30": pa.array(pmms30, type=pa.string()),
        "pmms30p": pa.array(pmms30p, type=pa.string()),
        "pmms15": pa.array(pmms15, type=pa.string()),
        "pmms15p": pa.array(pmms15p, type=pa.string()),
    })

    # Parse date from M/D/YYYY to YYYY-MM-DD
    dates = table.column("date").to_pylist()
    parsed_dates = []
    for d in dates:
        d = d.strip().strip('"')
        parts = d.split("/")
        month = parts[0].zfill(2)
        day = parts[1].zfill(2)
        year = parts[2]
        parsed_dates.append(f"{year}-{month}-{day}")

    # Convert rate columns - handle empty strings, spaces, and quotes
    def parse_rate(col):
        values = table.column(col).to_pylist()
        result = []
        for v in values:
            if v is None:
                result.append(None)
            else:
                v = v.strip().strip('"')
                if v == "":
                    result.append(None)
                else:
                    result.append(float(v))
        return pa.array(result, type=pa.float64())

    columns = {
        "date": pa.array(parsed_dates, type=pa.string()),
        "rate_30yr": parse_rate("pmms30"),
        "points_30yr": parse_rate("pmms30p"),
        "rate_15yr": parse_rate("pmms15"),
        "points_15yr": parse_rate("pmms15p"),
    }

    output = pa.table(columns)

    print(f"  {DATASET_ID}: {output.num_rows:,} rows")

    test(output)

    upload_data(output, DATASET_ID)
    publish(DATASET_ID, METADATA)


if __name__ == "__main__":
    run()
