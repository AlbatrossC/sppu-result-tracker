import re
import unicodedata
from datetime import date, datetime
from typing import Dict, List, Tuple

from bs4 import BeautifulSoup


class ParseError(RuntimeError):
    """Raised when a response is not a trustworthy SPPU result page."""


def normalize_course_name(value: str) -> str:
    normalized = unicodedata.normalize("NFKC", value)
    return re.sub(r"\s+", " ", normalized).strip()


def course_key(value: str) -> str:
    return normalize_course_name(value).casefold()


def parse_result_date(value: str) -> date:
    normalized = re.sub(r"\s*-\s*", "-", value.strip())
    for pattern in ("%d-%B-%Y", "%d-%b-%Y"):
        try:
            return datetime.strptime(normalized, pattern).date()
        except ValueError:
            continue
    raise ValueError(f"Unsupported result date: {value!r}")


def parse_html_content(html_content: str, minimum_count: int = 25) -> List[Dict[str, object]]:
    """Parse and validate the result table into normalized result records."""
    if not html_content or not html_content.strip():
        raise ParseError("The downloaded page is empty")

    soup = BeautifulSoup(html_content, "html.parser")
    table = soup.find("table", id="tblRVList")
    if table is None:
        raise ParseError("The expected SPPU result table (tblRVList) is missing")

    header_cells = table.find_all("th")
    headers = [normalize_course_name(cell.get_text(" ", strip=True)).casefold() for cell in header_cells]
    try:
        course_index = headers.index("course name")
        date_index = headers.index("result date")
    except ValueError as exc:
        raise ParseError("The SPPU result table headers have changed") from exc

    records: Dict[Tuple[str, date], Dict[str, object]] = {}
    malformed_rows = []
    data_rows = 0

    for row_number, row in enumerate(table.find_all("tr"), start=1):
        cells = row.find_all("td")
        if not cells:
            continue
        data_rows += 1
        if max(course_index, date_index) >= len(cells):
            malformed_rows.append(f"row {row_number}: missing columns")
            continue

        name = normalize_course_name(cells[course_index].get_text(" ", strip=True))
        raw_date = cells[date_index].get_text(" ", strip=True)
        try:
            parsed_date = parse_result_date(raw_date)
        except ValueError as exc:
            malformed_rows.append(f"row {row_number}: {exc}")
            continue

        if not name:
            malformed_rows.append(f"row {row_number}: empty course name")
            continue

        key = course_key(name)
        records[(key, parsed_date)] = {
            "course_key": key,
            "course_name": name,
            "result_date": parsed_date,
        }

    if malformed_rows:
        sample = "; ".join(malformed_rows[:3])
        raise ParseError(f"Found {len(malformed_rows)} malformed result rows: {sample}")
    if len(records) < minimum_count:
        raise ParseError(f"Only {len(records)} valid results were found; expected at least {minimum_count}")

    duplicate_count = data_rows - len(records)
    if data_rows and duplicate_count / data_rows > 0.02:
        raise ParseError(f"Duplicate result ratio is too high ({duplicate_count}/{data_rows})")

    return sorted(records.values(), key=lambda item: (item["course_key"], item["result_date"]))
