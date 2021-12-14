from functools import partial
from types import SimpleNamespace
from typing import Dict

from functions import clean_doc, connect, rows, solr_date, solr_load_batch


def build_doc(report: SimpleNamespace) -> Dict:
    """Build a report doc."""
    doc = {
        "id": "/reports?id=%s" % report.report_id,
        "atlas_id": report.report_id,
        "type": "reports",
        "source_server": report.system_server,
        "server_path": report.server_path,
        "source_database": report.system_db,
        "name": report.name,
        "description": [
            report.description,
            report.detailed_description,
            report.system_description,
            report.docs_description,
            report.docs_assumptions,
        ],
        "certification": report.certification_tag,
        "report_type": report.report_type,
        "report_type_id": report.report_type_id,
        "author": str(report.created_by),
        "report_last_updated_by": str(report.modified_by),
        "report_last_updated": solr_date(report.modified_at),
        "epic_master_file": report.system_identifier,
        "epic_record_id": str(report.system_id),
        "visible": report.visible,
        "orphan": report.orphan or "N",
        "runs": 10,
        "run_url": report.run_url,
        "epic_template": str(report.system_template_id),
        "last_load_date": solr_date(report.etl_date),
        "query": ([x for x in report.query.split("~|~") if x] if report.query else []),
        "fragility_tags": (
            [x for x in report.tag.split("~|~") if x] if report.tag else []
        ),
        "related_terms": (
            [x for x in report.term_name.split("~|~") if x] if report.term_name else []
        ),
        "linked_description": (
            [x for x in report.term_description.split("~|~") if x]
            if report.term_description
            else []
        )
        + (
            [x for x in report.collection_description.split("~|~") if x]
            if report.collection_description
            else []
        )
        + (
            [x for x in report.initiative_description.split("~|~") if x]
            if report.initiative_description
            else []
        ),
        "related_collections": (
            [x for x in report.collection_name.split("~|~") if x]
            if report.collection_name
            else []
        ),
        "related_initiatives": (
            [x for x in report.initiative_name.split("~|~") if x]
            if report.initiative_name
            else []
        ),
        "documented": report.documented,
        "do_not_purge": report.do_not_purge,
        "enabled_for_hyperspace": report.enabled_for_hyperspace,
        "updated_by": report.updated_by,
        "created_by": report.created_by,
        "last_updated": solr_date(report.last_updated),
        "maintenance_schedule": report.maintenance_schedule,
        "executive_visibility": report.executive_visibility,
        "fragility": report.fragility,
        "estimated_run_frequency": report.estimated_run_frequency,
        "organizational_value": report.organizational_value,
        "created": solr_date(report.created),
        "requester": report.requester,
        "operations_owner": report.ops_owner,
    }

    return clean_doc(doc)


cnxn, cursor = connect()

cursor.execute(
    """select --top 100
  r.ReportObjectID as report_id
, r.SourceServer as system_server
, r.ReportServerPath as server_path
, r.SourceDB as system_db
, isnull(r.displaytitle, r.name) as name
, r.description as description
, r.DetailedDescription as detailed_description
, r.RepositoryDescription as system_description
, tag.CertName as certification_tag
, isnull(report_type.ShortName, report_type.name) as report_type
, r.reportobjecttypeid as report_type_id
, author.Fullname as created_by
, updater.Fullname as modified_by
, r.LastModifiedDate as modified_at
, r.EpicMasterFile as system_identifier
, r.EpicRecordID as system_id
, case when isnull(r.OrphanedReportObjectYN, 'N') = 'N' and report_type.Visible = 'Y' and r.DefaultVisibilityYN = 'Y' and isnull(d.Hidden,'N') = 'N' then 'Y' else 'N' end as visible
, isnull(r.OrphanedReportObjectYN, 'N') as orphan
, r.EpicReportTemplateId as system_template_id
, r.LastLoadDate as etl_date
, r.ReportObjectUrl as run_url
, STUFF((select '~|~' +  q.Query from dbo.ReportObjectQuery q where q.ReportObjectId = r.ReportObjectID  FOR XML PATH('')), 1, 3, '') query 
, STUFF((select '~|~' +  f.FragilityTagName from app.ReportObjectDocFragilityTags t inner join app.FragilityTag f on t.FragilityTagID = f.FragilityTagID where t.ReportObjectId = r.ReportObjectID  FOR XML PATH('')), 1, 3, '') tag 


, case when isnull(cast(d.ReportObjectID as nvarchar),'N') = 'N' then 'N' else 'Y' end as documented
, d.DoNotPurge as do_not_purge
, d.EnabledForHyperspace  as enabled_for_hyperspace
, doc_updater.Fullname as updated_by
, doc_creator.Fullname as created_by
, d.LastUpdateDateTime as last_updated
, ms.MaintenanceScheduleName as maintenance_schedule
, isnull(d.ExecutiveVisibilityYN, 'N') as executive_visibility
, fr.FragilityName as fragility
, rf.EstimatedRunFrequencyName as estimated_run_frequency
, ov.OrganizationalValueName as organizational_value
, d.CreatedDateTime as created
, requester.Fullname as requester
, ops_owner.Fullname as ops_owner
, d.DeveloperDescription as docs_description
, d.KeyAssumptions as docs_assumptions

, STUFF((select '~|~' +  t.name from app.ReportObjectDocTerms dt inner join app.Term t on dt.TermId = t.termid where dt.reportobjectid = r.ReportObjectID  FOR XML PATH('')), 1, 3, '') term_name 
, STUFF((select '~|~' +  t.Summary + '~|~' + t.TechnicalDefinition from app.ReportObjectDocTerms dt inner join app.Term t on dt.TermId = t.termid where dt.reportobjectid = r.ReportObjectID  FOR XML PATH('')), 1, 3, '') term_description

, STUFF((select '~|~' +  p.name from app.DP_ReportAnnotation a inner join app.DP_DataProject p on a.DataProjectId = p.DataProjectID where a.ReportId = r.ReportObjectID and isnull(Hidden,'N')='N' FOR XML PATH('')), 1, 3, '') collection_name 
, STUFF((select '~|~' +  p.Description + '~|~' + p.Purpose from app.DP_ReportAnnotation a inner join app.DP_DataProject p on a.DataProjectId = p.DataProjectID where a.ReportId = r.ReportObjectID and isnull(Hidden,'N')='N' FOR XML PATH('')), 1, 3, '') collection_description

, STUFF((select '~|~' +  i.name from app.DP_ReportAnnotation a inner join app.DP_DataProject p on a.DataProjectId = p.DataProjectID inner join app.DP_DataInitiative i on i.DataInitiativeID=p.DataInitiativeID where a.ReportId = r.ReportObjectID and isnull(Hidden,'N')='N' FOR XML PATH('')), 1, 3, '') initiative_name 
, STUFF((select '~|~' +  i.Description + '~|~' + p.Purpose from app.DP_ReportAnnotation a inner join app.DP_DataProject p on a.DataProjectId = p.DataProjectID inner join app.DP_DataInitiative i on i.DataInitiativeID=p.DataInitiativeID where a.ReportId = r.ReportObjectID and isnull(Hidden,'N')='N' FOR XML PATH('')), 1, 3, '') initiative_description



from dbo.ReportObject r
left outer join dbo.ReportCertificationTags tag on r.CertificationTagID = tag.Cert_ID
left outer join app.ReportObject_doc d on r.ReportObjectID = d.ReportObjectID
left outer join dbo.ReportObjectType report_type on r.ReportObjectTypeID = report_type.ReportObjectTypeID
left outer join app.User_NameData as author on r.AuthorUserID = author.UserId
left outer join app.User_NameData as updater on r.LastModifiedByUserID = updater.UserId
left outer join app.User_NameData as doc_updater on d.UpdatedBy = doc_updater.UserId
left outer join app.User_NameData as doc_creator on d.UpdatedBy = doc_creator.UserId
left outer join app.MaintenanceSchedule ms on ms.MaintenanceScheduleID = d.MaintenanceScheduleID
left outer join app.Fragility fr on fr.FragilityID = d.FragilityID
left outer join app.EstimatedRunFrequency rf on rf.EstimatedRunFrequencyID = d.EstimatedRunFrequencyID
left outer join app.OrganizationalValue ov on ov.OrganizationalValueID = d.OrganizationalValueID
left outer join app.User_NameData as requester on requester.UserId = d.Requester
left outer join app.User_NameData as ops_owner on ops_owner.UserId = d.OperationalOwnerUserID
"""
)

columns = [column[0] for column in cursor.description]

batch_loader = partial(solr_load_batch, build_doc)
list(map(batch_loader, rows(cursor, columns)))

cnxn.close()
