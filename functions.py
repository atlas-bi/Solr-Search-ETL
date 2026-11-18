"""Atlas Solr ETL shared functions."""

import os
import re
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any, Dict, Generator, Iterable, Iterator, List, Optional, Tuple, Union

import pyodbc
import pysolr
from dotenv import load_dotenv

load_dotenv()

# Max approximate content size of one solr request (5 MB).
MAX_BATCH_BYTES = 5 * 1024 * 1024
# Rows fetched from the database per round trip.
FETCH_SIZE = 500

ATLASDATABASE = os.environ.get(
    "ATLASDATABASE",
    "DRIVER={ODBC Driver 18 for SQL Server};SERVER=atlas_server;DATABASE=atlas;UID=mr_cool;PWD=12345;TrustServerCertificate=Yes;",
)


def clean_doc(doc: Dict) -> Dict:
    """Clean up a solr doc list."""

    def clean_list(my_list: Any) -> Union[List[Any], Any]:
        """Remove None and "None" values from a list."""
        if not isinstance(my_list, list):
            return my_list

        return [i for i in my_list if i and i != "None"] or None

    return {
        k: clean_list(v) for k, v in doc.items() if clean_list(v) not in [None, "None"]
    }


def connect() -> Tuple[Any, Any]:
    """Create sql connection."""
    cnxn = pyodbc.connect(
        ATLASDATABASE,
        timeout=4,
    )
    return cnxn, cnxn.cursor()


def solr_load_batch(build_doc: Dict, url: str, batch: List) -> None:
    """Process batch."""
    solr = pysolr.Solr(url, always_commit=True)
    solr.add(list(map(build_doc, batch)))


def row_size(row: Tuple) -> int:
    """Approximate the number of bytes a row contributes to a solr request."""
    return sum(len(str(value).encode("utf-8")) for value in row if value is not None)


def _fetch_rows(cursor: Any, fetch_size: int) -> Iterator[Tuple]:
    """Yield raw rows from the cursor, fetching from the db fetch_size at a time."""
    while True:
        fetched_rows = cursor.fetchmany(fetch_size)

        if not fetched_rows:
            return

        yield from fetched_rows


def _batch_by_bytes(row_iter: Iterable[Tuple], max_bytes: int) -> Iterator[List[Tuple]]:
    """Group rows into batches whose combined size stays under max_bytes.

    A single row larger than max_bytes is still yielded, alone in its own batch.
    """
    batch: List[Tuple] = []
    batch_bytes = 0

    for row in row_iter:
        row_bytes = row_size(row)

        if batch and batch_bytes + row_bytes > max_bytes:
            yield batch
            batch = []
            batch_bytes = 0

        batch.append(row)
        batch_bytes += row_bytes

    if batch:
        yield batch


def rows(
    cursor: Any,
    columns: List[str],
    max_bytes: int = MAX_BATCH_BYTES,
    fetch_size: int = FETCH_SIZE,
) -> Generator:
    """Return data from query by a generator of batches capped by content size."""
    sent = 0

    for batch in _batch_by_bytes(_fetch_rows(cursor, fetch_size), max_bytes):
        print(f"records {sent}-{sent + len(batch)}")  # noqa: T201
        sent += len(batch)

        yield [SimpleNamespace(**dict(zip(columns, row))) for row in batch]


def clean_description(text: Optional[str]) -> Optional[str]:
    """Remove bad characters from string."""
    if text is None:
        return text
    return re.sub(r"<.*?>", "", text)


def solr_date(date: Any) -> Optional[str]:
    """Convert datetime to solr date format."""
    if isinstance(date, datetime):
        # this will still fail on invalid dates, like 9999-01-01
        try:
            return datetime.strftime(
                date.astimezone(timezone.utc),
                "%Y-%m-%dT%H:%M:%SZ",
            )

        except:  # noqa: B001,E722
            return datetime.strftime(
                date,
                "%Y-%m-%dT%H:%M:%SZ",
            )
    return None
