"""Transform Freddie Mac House Price Index data."""

import pyarrow as pa
import pyarrow.csv as csv
import pyarrow.compute as pc
from subsets_utils import load_raw_json, upload_data, publish
from .test import test

DATASET_ID = "freddie_mac_house_price_index"

METADATA = {
    "id": DATASET_ID,
    "title": "Freddie Mac House Price Index",
    "description": "Monthly house price index for US states, MSAs, and nationwide. Includes seasonally adjusted and non-seasonally adjusted indices.",
    "column_descriptions": {
        "month": "Month of observation (YYYY-MM)",
        "geo_type": "Geographic type (State, MSA, US)",
        "geo_code": "Geographic code (FIPS code for states, CBSA code for MSAs)",
        "geo_name": "Geographic area name",
        "index_nsa": "House Price Index (non-seasonally adjusted)",
        "index_sa": "House Price Index (seasonally adjusted)",
    }
}


def run():
    """Transform Freddie Mac House Price Index data."""
    raw_data = load_raw_json("data_sources")
    csv_text = raw_data["freddie_mac"]

    table = csv.read_csv(pa.py_buffer(csv_text.encode()))

    # Build month column from Year and Month
    year_str = pc.cast(table.column("Year"), pa.string())
    month_str = pc.utf8_lpad(pc.cast(table.column("Month"), pa.string()), 2, "0")
    month_col = pc.binary_join_element_wise(year_str, month_str, "-")

    # Clean geo_code - replace "." with null
    geo_code = table.column("GEO_Code")
    geo_code = pc.cast(geo_code, pa.string())
    geo_code = pc.if_else(pc.equal(geo_code, "."), None, geo_code)

    columns = {
        "month": month_col,
        "geo_type": table.column("GEO_Type"),
        "geo_code": geo_code,
        "geo_name": table.column("GEO_Name"),
        "index_nsa": pc.cast(table.column("Index_NSA"), pa.float64()),
        "index_sa": pc.cast(table.column("Index_SA"), pa.float64()),
    }

    output = pa.table(columns)

    print(f"  {DATASET_ID}: {output.num_rows:,} rows")

    test(output)

    upload_data(output, DATASET_ID)
    publish(DATASET_ID, METADATA)


if __name__ == "__main__":
    run()
