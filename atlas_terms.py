from functools import partial
from types import SimpleNamespace
from typing import Dict

from functions import clean_doc, connect, rows, solr_date, solr_load_batch


def build_doc(term: SimpleNamespace) -> Dict:
    """Build term doc."""
    doc = {
        "id": "/terms?id=%s" % term.term_id,
        "atlas_id": term.term_id,
        "type": "terms",
        "name": term.name,
        "visible": "Y",
        "orphan": "N",
        "runs": 10,
        "description": [term.summary, term.technical_definition],
        "approved": term.approved or "N",
        "approval_date": solr_date(term.approved_at),
        "approved_by": str(term.approved_by),
        "has_external_standard": "Y" if bool(term.has_external_standard) else "N",
        "external_url": term.has_external_standard,
        "valid_from": solr_date(term.valid_from),
        "valid_to": solr_date(term.valid_to),
        "last_updated": solr_date(term.modified_at),
        "updated_by": str(term.modified_by),
        "related_collections": (
            [x for x in term.collection_name.split("~|~") if x]
            if term.collection_name
            else []
        ),
        "related_initiatives": (
            [x for x in term.initiative_name.split("~|~") if x]
            if term.initiative_name
            else []
        ),
        "related_terms": [],  # keep blank
        "related_reports": (
            [x for x in term.report_name.split("~|~") if x] if term.report_name else []
        ),
        "linked_name": (
            [x for x in term.linked_name.split("~|~") if x] if term.linked_name else []
        ),
        "linked_description": (
            [x for x in term.collection_description.split("~|~") if x]
            if term.collection_description
            else []
        )
        + (
            [x for x in term.initiative_description.split("~|~") if x]
            if term.initiative_description
            else []
        )
        + (
            [x for x in term.linked_description.split("~|~") if x]
            if term.linked_description
            else []
        ),
    }

    return clean_doc(doc)


cnxn, cursor = connect()

cursor.execute(
    """
select
  termid as term_id
, name as name
, summary as summary
, TechnicalDefinition as technical_definition
, isnull(approvedYN,'N') as approved
, ApprovalDateTime as approved_at
, approver.Fullname as approved_by
, case when ExternalStandardUrl is null then 'N' else 'Y' end as has_external_standard
, ExternalStandardUrl as external_url
, ValidFromDateTime as valid_from
, ValidToDateTime as valid_to
, t.LastUpdatedDateTime as modified_at
, updater.Fullname as modified_by

, STUFF((select '~|~' +  p.name from app.DP_TermAnnotation a inner join app.DP_DataProject p on a.DataProjectId=p.DataProjectID where a.TermId=t.TermId FOR XML PATH('')), 1, 3, '') collection_name 
, STUFF((select '~|~' +  p.description from app.DP_TermAnnotation a inner join app.DP_DataProject p on a.DataProjectId=p.DataProjectID where a.TermId=t.TermId FOR XML PATH('')), 1, 3, '') collection_description

, STUFF((select '~|~' +  i.name from app.DP_TermAnnotation a inner join app.DP_DataProject p on a.DataProjectId=p.DataProjectID inner join app.DP_DataInitiative i on i.DataInitiativeID=p.datainitiativeid where a.TermId=t.TermId FOR XML PATH('')), 1, 3, '') initiative_name 
, STUFF((select '~|~' +  i.description from app.DP_TermAnnotation a inner join app.DP_DataProject p on a.DataProjectId=p.DataProjectID inner join app.DP_DataInitiative i on i.DataInitiativeID=p.datainitiativeid where a.TermId=t.TermId FOR XML PATH('')), 1, 3, '') initiative_description

, STUFF((select '~|~' +  r.name from app.ReportObjectDocTerms a inner join dbo.ReportObject r on a.ReportObjectID=r.ReportObjectID where a.TermId=t.TermId FOR XML PATH('')), 1, 3, '') report_name 
, STUFF((select '~|~' +  r.DisplayTitle from app.ReportObjectDocTerms a inner join dbo.ReportObject r on a.ReportObjectID=r.ReportObjectID where a.TermId=t.TermId and r.DisplayTitle is not null FOR XML PATH('')), 1, 3, '') linked_name 

, STUFF((select '~|~' +  d.DeveloperDescription + '~|~' + d.KeyAssumptions  + '~|~' + r.Description +  '~|~' + r.DetailedDescription + '~|~' + r.RepositoryDescription from app.ReportObjectDocTerms a inner join dbo.ReportObject r on a.ReportObjectID=r.ReportObjectID inner join app.ReportObject_doc d on r.reportobjectid = d.reportobjectid where a.TermId=t.TermId and r.DisplayTitle is not null FOR XML PATH('')), 1, 3, '') linked_description



from app.Term t
left outer join app.User_NameData approver on t.ApprovedByUserId = approver.UserId
left outer join app.User_NameData updater on t.UpdatedByUserId = updater.UserId

"""
)

columns = [column[0] for column in cursor.description]

batch_loader = partial(solr_load_batch, build_doc)
list(map(batch_loader, rows(cursor, columns)))

cnxn.close()
