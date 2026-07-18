from datetime import date
from pathlib import Path

import pytest

from src.parse import ParseError, parse_html_content, parse_result_date


FIXTURE = Path(__file__).with_name("sppu_result_page.html")


def test_saved_sppu_page_has_expected_results():
    records = parse_html_content(FIXTURE.read_text(encoding="utf-8"))

    assert len(records) == 259
    assert len({(row["course_key"], row["result_date"]) for row in records}) == 259


def test_result_date_parser_validates_real_dates():
    assert parse_result_date("08- November- 2025") == date(2025, 11, 8)
    with pytest.raises(ValueError):
        parse_result_date("99- November- 2025")


def test_missing_expected_table_is_rejected():
    with pytest.raises(ParseError, match="tblRVList"):
        parse_html_content("<html><table></table></html>", minimum_count=1)


def test_malformed_result_row_is_rejected():
    html = """
    <table id="tblRVList">
      <tr><th>#</th><th>Course Name</th><th>Result Date</th></tr>
      <tr><td>1</td><td>Test Course</td><td>not-a-date</td></tr>
    </table>
    """
    with pytest.raises(ParseError, match="malformed"):
        parse_html_content(html, minimum_count=1)


def test_excessive_duplicates_are_rejected():
    html = """
    <table id="tblRVList">
      <tr><th>#</th><th>Course Name</th><th>Result Date</th></tr>
      <tr><td>1</td><td>Test Course</td><td>08- November- 2025</td></tr>
      <tr><td>2</td><td>Test Course</td><td>08- November- 2025</td></tr>
    </table>
    """
    with pytest.raises(ParseError, match="Duplicate"):
        parse_html_content(html, minimum_count=1)
