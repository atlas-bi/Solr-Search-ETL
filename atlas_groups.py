"""Load atlas users to solr search."""
from functools import partial
from types import SimpleNamespace
from typing import Dict

from functions import clean_doc, connect, rows, solr_load_batch


def build_doc(group: SimpleNamespace) -> Dict:
    """Build group doc."""
    doc = {
        "id": "/groups?id=%s" % group.group_id,
        "atlas_id": group.group_id,
        "type": "groups",
        "name": group.name,
        "email": group.email,
        "group_type": group.group_type,
        "group_source": group.group_source,
        "visible": "Y",
        "orphan": "N",
        "runs": 10,
    }

    return clean_doc(doc)


cnxn, cursor = connect()

cursor.execute(
    """
select
  g.groupid as group_id
, groupname as name
, groupemail as email
, grouptype as group_type
, groupsource as group_source
from dbo.UserGroups g"""
)

columns = [column[0] for column in cursor.description]

batch_loader = partial(solr_load_batch, build_doc)
list(map(batch_loader, rows(cursor, columns)))

cnxn.close()
