from functools import partial
from types import SimpleNamespace
from typing import Dict

from functions import clean_doc, connect, rows, solr_date, solr_load_batch


def build_doc(collection: SimpleNamespace) -> Dict:
    """Build a collection doc."""
    doc = {
        "id": "/collections?id=%s" % collection.collection_id,
        "atlas_id": collection.collection_id,
        "type": "collections",
        "name": collection.name,
        "visible": "Y",
        "orphan": "N",
        "runs": 10,
        "description": [collection.search_summary, collection.description],
        "last_updated": solr_date(collection.modified_at),
        "updated_by": str(collection.modified_by),
        "related_initiatives": (
            [x for x in collection.initiative_name.split("~|~") if x]
            if collection.initiative_name
            else []
        ),
        "related_terms": (
            [x for x in collection.term_name.split("~|~") if x]
            if collection.term_name
            else []
        ),
        "related_reports": (
            [x for x in collection.report_name.split("~|~") if x]
            if collection.report_name
            else []
        ),
        "linked_name": (
            [x for x in collection.linked_name.split("~|~") if x]
            if collection.linked_name
            else []
        ),
        "linked_description": (
            [x for x in collection.initiative_description.split("~|~") if x]
            if collection.initiative_description
            else []
        )
        + (
            [x for x in collection.term_description.split("~|~") if x]
            if collection.term_description
            else []
        )
        + (
            [x for x in collection.linked_description.split("~|~") if x]
            if collection.linked_description
            else []
        ),
    }

    return clean_doc(doc)


cnxn, cursor = connect()

cursor.execute(
    """select
  p.DataProjectID as collection_id
, p.name as name
, p.Purpose as search_summary
, p.Description as description
, p.LastUpdateDate as modified_at
, updater.Fullname as modified_by

, STUFF((select '~|~' +  i.name from app.DP_DataInitiative i where i.DataInitiativeID=p.datainitiativeid  FOR XML PATH('')), 1, 3, '') initiative_name 
, STUFF((select '~|~' +  i.description from app.DP_DataInitiative i where i.DataInitiativeID=p.datainitiativeid FOR XML PATH('')), 1, 3, '') initiative_description


, STUFF((select '~|~' +  t.name from app.DP_TermAnnotation a inner join app.term t on a.TermId = t.termid where a.DataProjectId=p.DataProjectID  FOR XML PATH('')), 1, 3, '') term_name 
, STUFF((select '~|~' +  t.Summary + '~|~' + t.TechnicalDefinition from app.DP_TermAnnotation a inner join app.term t on a.termid = t.termid where a.DataProjectId=p.DataProjectID FOR XML PATH('')), 1, 3, '')term_description



, STUFF((select '~|~' +  r.name from app.DP_ReportAnnotation a inner join dbo.ReportObject r on a.ReportId = r.ReportObjectID  where a.DataProjectId=p.DataProjectID  FOR XML PATH('')), 1, 3, '') report_name 
, STUFF((select '~|~' +  r.DisplayTitle from app.DP_ReportAnnotation a inner join dbo.ReportObject r on a.ReportId = r.ReportObjectID  where a.DataProjectId=p.DataProjectID and r.DisplayTitle <> NULL FOR XML PATH('')), 1, 3, '') linked_name 
, STUFF((select '~|~' +  r.Description + '~|~' + r.DetailedDescription + '~|~' + r.RepositoryDescription + '~|~' + d.DeveloperDescription + '~|~' + d.KeyAssumptions from app.DP_ReportAnnotation a inner join dbo.ReportObject r on a.ReportId = r.ReportObjectID left outer join app.ReportObject_doc d on r.ReportObjectID = d.ReportObjectID where a.DataProjectId=p.DataProjectID  FOR XML PATH('')), 1, 3, '') linked_description 


from app.DP_DataProject p
left outer join app.User_NameData updater on p.LastUpdateUser = updater.UserId
"""
)

columns = [column[0] for column in cursor.description]

batch_loader = partial(solr_load_batch, build_doc)
list(map(batch_loader, rows(cursor, columns)))

cnxn.close()
