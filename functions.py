"""Atlas Solr ETL shared functions."""
import itertools
import os
import re
from datetime import datetime
from types import SimpleNamespace
from typing import Any, Dict, Generator, List, Optional, Tuple, Union

import pyodbc
import pysolr
import pytz
from dotenv import load_dotenv

load_dotenv()
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


def rows(cursor: Any, columns: List[str], size: int = 500) -> Generator:
    """Return data from query by a generator."""
    for iteration in itertools.count():
        fetched_rows = cursor.fetchmany(size)

        if not fetched_rows:
            break

        start = size * iteration
        end = size * iteration + len(fetched_rows)
        print(f"records {start}-{end}")  # noqa: T201

        yield [SimpleNamespace(**dict(zip(columns, row))) for row in fetched_rows]


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
                date.astimezone(pytz.utc),
                "%Y-%m-%dT%H:%M:%SZ",
            )

        except:  # noqa: B001,E722
            return datetime.strftime(
                date,
                "%Y-%m-%dT%H:%M:%SZ",
            )
    return None
