"""Tests for Freddie Mac PMMS mortgage rates dataset."""

import pyarrow as pa
from subsets_utils import validate
from subsets_utils.testing import assert_valid_date, assert_in_range


def test(table: pa.Table) -> None:
    """Validate PMMS mortgage rates dataset."""
    validate(table, {
        "columns": {
            "date": "string",
            "rate_30yr": "double",
            "points_30yr": "double",
            "rate_15yr": "double",
            "points_15yr": "double",
        },
        "not_null": ["date", "rate_30yr"],
        "min_rows": 2000,
    })

    assert_valid_date(table, "date")
    assert_in_range(table, "rate_30yr", 0, 20)
    assert_in_range(table, "rate_15yr", 0, 20)
    assert_in_range(table, "points_30yr", 0, 10)
    assert_in_range(table, "points_15yr", 0, 10)

    # Validate date range - data starts 1971
    dates = table.column("date").to_pylist()
    years = [int(d[:4]) for d in dates]
    assert min(years) >= 1971, f"Data starts too early: {min(years)}"
    assert max(years) <= 2030, f"Future dates found: {max(years)}"

    # 15-year rates started later (1991)
    rate_15yr = table.column("rate_15yr").to_pylist()
    non_null_15yr = [r for r in rate_15yr if r is not None]
    assert len(non_null_15yr) > 1000, f"Too few 15-year rates: {len(non_null_15yr)}"
