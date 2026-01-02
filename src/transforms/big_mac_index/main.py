"""Transform Big Mac Index data."""

import pyarrow as pa
import pyarrow.csv as csv
import pyarrow.compute as pc
from subsets_utils import load_raw_json, upload_data, publish
from .test import test

DATASET_ID = "big_mac_index"

METADATA = {
    "id": DATASET_ID,
    "title": "Big Mac Index",
    "description": "The Economist's Big Mac Index - an informal measure of purchasing power parity between currencies. Includes raw and GDP-adjusted indices against USD, EUR, GBP, JPY, and CNY.",
    "column_descriptions": {
        "date": "Date of observation (YYYY-MM-DD)",
        "country_code": "ISO 3-letter country code",
        "country_name": "Country name",
        "currency_code": "Local currency code",
        "local_price": "Local price of a Big Mac in local currency",
        "exchange_rate": "Exchange rate to USD",
        "dollar_price": "Price in USD",
        "usd_raw_index": "Raw index vs USD (% over/under valued)",
        "eur_raw_index": "Raw index vs EUR (% over/under valued)",
        "gbp_raw_index": "Raw index vs GBP (% over/under valued)",
        "jpy_raw_index": "Raw index vs JPY (% over/under valued)",
        "cny_raw_index": "Raw index vs CNY (% over/under valued)",
        "gdp_bigmac": "GDP per capita, Big Mac-adjusted",
        "adjusted_price": "GDP-adjusted price",
        "usd_adjusted_index": "GDP-adjusted index vs USD",
        "eur_adjusted_index": "GDP-adjusted index vs EUR",
        "gbp_adjusted_index": "GDP-adjusted index vs GBP",
        "jpy_adjusted_index": "GDP-adjusted index vs JPY",
        "cny_adjusted_index": "GDP-adjusted index vs CNY",
    }
}

COLUMN_MAPPING = {
    "iso_a3": "country_code",
    "currency_code": "currency_code",
    "name": "country_name",
    "date": "date",
    "local_price": "local_price",
    "dollar_ex": "exchange_rate",
    "dollar_price": "dollar_price",
    "USD_raw": "usd_raw_index",
    "EUR_raw": "eur_raw_index",
    "GBP_raw": "gbp_raw_index",
    "JPY_raw": "jpy_raw_index",
    "CNY_raw": "cny_raw_index",
    "GDP_bigmac": "gdp_bigmac",
    "adj_price": "adjusted_price",
    "USD_adjusted": "usd_adjusted_index",
    "EUR_adjusted": "eur_adjusted_index",
    "GBP_adjusted": "gbp_adjusted_index",
    "JPY_adjusted": "jpy_adjusted_index",
    "CNY_adjusted": "cny_adjusted_index",
}


def run():
    """Transform Big Mac Index data."""
    raw_data = load_raw_json("data_sources")
    csv_text = raw_data["big_mac_index"]

    table = csv.read_csv(pa.py_buffer(csv_text.encode()))

    # Select and rename columns
    available = [col for col in COLUMN_MAPPING.keys() if col in table.column_names]
    table = table.select(available)
    table = table.rename_columns([COLUMN_MAPPING[col] for col in available])

    # Convert date from date32 to string
    date_col = pc.strftime(table.column("date"), format="%Y-%m-%d")
    table = table.set_column(table.schema.get_field_index("date"), "date", date_col)

    # Filter out rows with missing essential values
    for col in ["country_code", "date", "dollar_price"]:
        if col in table.column_names:
            table = table.filter(pc.is_valid(table[col]))

    print(f"  {DATASET_ID}: {table.num_rows:,} rows")

    test(table)

    upload_data(table, DATASET_ID)
    publish(DATASET_ID, METADATA)


if __name__ == "__main__":
    run()
