import itertools
from datetime import datetime
from types import SimpleNamespace
from typing import Any, Dict, Generator, List, Optional, Tuple, Union

import pyodbc
import pysolr
import pytz
import settings


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
        f"DRIVER={{ODBC Driver 17 for SQL Server}};{settings.SQL_CONN}",
        timeout=4,
    )
    return cnxn, cnxn.cursor()


def solr_load_batch(build_doc: Dict, batch: List) -> None:
    """Process batch."""
    solr = pysolr.Solr(settings.SOLR_URL, always_commit=True)
    solr.add(list(map(build_doc, batch)))


def rows(cursor: Any, columns: List[str], size: int = 500) -> Generator:
    """Return data from query by a generator."""
    for iteration in itertools.count():

        fetched_rows = cursor.fetchmany(size)

        if not fetched_rows:
            break

        start = size * iteration
        end = size * iteration + len(fetched_rows)
        print(f"records {start}-{end}")  # noqa: T001

        yield [SimpleNamespace(**dict(zip(columns, row))) for row in fetched_rows]


def solr_date(date: Any) -> Optional[str]:
    """Convert datetime to solr date format."""
    if isinstance(date, datetime):
        return datetime.strftime(
            date.astimezone(pytz.utc),
            "%Y-%m-%dT%H:%M:%SZ",
        )

    return None
