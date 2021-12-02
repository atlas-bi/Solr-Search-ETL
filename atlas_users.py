"""Load atlas users to solr search."""
from functools import partial
from types import SimpleNamespace
from typing import Dict

from functions import clean_doc, connect, rows, solr_load_batch


def build_doc(user: SimpleNamespace) -> Dict:
    """Build user doc."""
    doc = {
        "id": "/users?id=%s" % user.user_id,
        "atlas_id": user.user_id,
        "type": "users",
        "name": user.name,
        "employee_id": user.employee_id,
        "email": user.email,
        "epic_record_id": user.system_id,
        "user_roles": (
            [x.strip() for x in user.roles.split("|")] if user.roles else []
        ),
        "user_groups": (
            [x.strip() for x in user.groups.split("|")] if user.groups else []
        ),
        "visible": user.visible,
        "orphan": "N",
        "runs": 10,
    }

    return clean_doc(doc)


cnxn, cursor = connect()

cursor.execute(
    """select u.userid as user_id
, isnull(d.fullname, u.accountname) as name
, employeeid as employee_id
, email as email
, epicid as system_id
, , case when isnull((select Value from app.GlobalSiteSettings where Name = 'users_search_visibility'),'N') = 'N' then 'N' else 'Y' end as visible
, STUFF((select '|' +  r.name from app.UserRoleLinks l inner join app.userRoles r on l.UserRolesId = r.UserRolesId where l.userid=u.userid FOR XML PATH('')), 1, 1, '') roles

, stuff((select '|' + g.GroupName from dbo.UserGroups g inner join dbo.UserGroupsMembership m on g.GroupId = m.GroupId where m.UserId = u.userid FOR XML PATH('')), 1, 1, '') groups

from dbo.[user] u
left outer join app.user_namedata d on u.userid = d.userid
"""
)

columns = [column[0] for column in cursor.description]

batch_loader = partial(solr_load_batch, build_doc)
list(map(batch_loader, rows(cursor, columns)))

cnxn.close()
