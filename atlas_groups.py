"""Load atlas users to solr search."""
from functools import partial
from types import SimpleNamespace
from typing import Dict

import settings
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
        "visible": group.visible,
        "orphan": "N",
        "runs": 10,
        "linked_name": (
            [x for x in group.users.split("~|~") if x] if group.users else []
        ),
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
, case when isnull((select Value from app.GlobalSiteSettings where Name = 'groups_search_visibility'),'N') = 'N' then 'N' else 'Y' end as visible
, STUFF((select '~|~' +  u.FullName_calc from dbo.[User] u inner join dbo.UserGroupsMembership m on u.UserId = m.UserId where m.GroupId = g.GroupId  FOR XML PATH('')), 1, 3, '') users

from dbo.UserGroups g"""
)

columns = [column[0] for column in cursor.description]

batch_loader = partial(solr_load_batch, build_doc, settings.SOLR_URL)
list(map(batch_loader, rows(cursor, columns)))

cnxn.close()
