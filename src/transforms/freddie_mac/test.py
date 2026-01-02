"""Tests for Freddie Mac House Price Index dataset."""

import pyarrow as pa
from subsets_utils import validate
from subsets_utils.testing import assert_valid_month, assert_positive, assert_in_set


def test(table: pa.Table) -> None:
    """Validate Freddie Mac House Price Index dataset."""
    validate(table, {
        "columns": {
            "month": "string",
            "geo_type": "string",
            "geo_code": "string",
            "geo_name": "string",
            "index_nsa": "double",
            "index_sa": "double",
        },
        "not_null": ["month", "geo_type", "geo_name"],
        "min_rows": 10000,
    })

    assert_valid_month(table, "month")
    assert_in_set(table, "geo_type", {"State", "MSA", "CBSA", "US"})
    assert_positive(table, "index_nsa")
    assert_positive(table, "index_sa")

    # Validate date range
    months = table.column("month").to_pylist()
    years = [int(m[:4]) for m in months]
    assert min(years) >= 1975, f"Data starts too early: {min(years)}"
    assert max(years) <= 2030, f"Future dates found: {max(years)}"
