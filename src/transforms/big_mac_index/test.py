"""Tests for Big Mac Index dataset."""

import pyarrow as pa
from subsets_utils import validate
from subsets_utils.testing import assert_valid_date, assert_max_length, assert_positive


def test(table: pa.Table) -> None:
    """Validate Big Mac Index dataset."""
    validate(table, {
        "columns": {
            "date": "string",
            "country_code": "string",
            "country_name": "string",
            "currency_code": "string",
            "local_price": "double",
            "exchange_rate": "double",
            "dollar_price": "double",
            "usd_raw_index": "double",
            "eur_raw_index": "double",
            "gbp_raw_index": "double",
            "jpy_raw_index": "double",
            "cny_raw_index": "double",
            "gdp_bigmac": "double",
            "adjusted_price": "double",
            "usd_adjusted_index": "double",
            "eur_adjusted_index": "double",
            "gbp_adjusted_index": "double",
            "jpy_adjusted_index": "double",
            "cny_adjusted_index": "double",
        },
        "not_null": ["date", "country_code", "dollar_price"],
        "min_rows": 100,
    })

    assert_valid_date(table, "date")
    assert_max_length(table, "country_code", 3)
    assert_max_length(table, "currency_code", 3)
    assert_positive(table, "dollar_price")
    assert_positive(table, "local_price")

    # Validate date range
    dates = table.column("date").to_pylist()
    years = [int(d[:4]) for d in dates]
    assert min(years) >= 2000, f"Data starts too early: {min(years)}"
    assert max(years) <= 2030, f"Future dates found: {max(years)}"
