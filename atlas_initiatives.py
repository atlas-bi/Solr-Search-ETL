"""Atlas Solr ETL for Initiatives."""
import os
from functools import partial
from types import SimpleNamespace
from typing import Dict

from dotenv import load_dotenv

load_dotenv()

SOLRURL = os.environ.get("SOLRURL", "https://solr.example.com/solr/atlas")

from functions import (
    clean_description,
    clean_doc,
    connect,
    rows,
    solr_date,
    solr_load_batch,
)


def build_doc(initiative: SimpleNamespace) -> Dict:
    """Build initiative doc."""
    doc = {
        "id": "/initiatives?id=%s" % initiative.initiative_id,
        "atlas_id": initiative.initiative_id,
        "type": "initiatives",
        "name": initiative.name,
        "visible": initiative.visible,
        "orphan": "N",
        "runs": 10,
        "operations_owner": str(initiative.ops_owner),
        "description": clean_description(initiative.description),
        "executive_owner": str(initiative.exec_owner),
        "financial_impact": str(initiative.financial_impact),
        "strategic_importance": str(initiative.strategic_importance),
        "last_updated": solr_date(initiative.modified_at),
        "updated_by": str(initiative.modified_by),
        "related_collections": (
            [x for x in initiative.collection_name.split("~|~") if x]
            if initiative.collection_name
            else []
        ),
        "linked_description": (
            [
                clean_description(x)
                for x in initiative.collection_description.split("~|~")
                if x
            ]
            if initiative.collection_description
            else []
        )
        + (
            [
                clean_description(x)
                for x in initiative.term_description.split("~|~")
                if x
            ]
            if initiative.term_description
            else []
        )
        + (
            [
                clean_description(x)
                for x in initiative.report_description.split("~|~")
                if x
            ]
            if initiative.report_description
            else []
        ),
        "related_terms": (
            [x for x in initiative.term_name.split("~|~") if x]
            if initiative.term_name
            else []
        ),
        "related_reports": (
            [x for x in initiative.report_name.split("~|~") if x]
            if initiative.report_name
            else []
        ),
        "linked_name": (
            [x for x in initiative.linked_name.split("~|~") if x]
            if initiative.linked_name
            else []
        ),
    }

    return clean_doc(doc)


cnxn, cursor = connect()

cursor.execute(
    """select
  i.DataInitiativeID as initiative_id
, i.name as name
, ops_owner.Fullname_calc as ops_owner
, description as description
, exec_owner.Fullname_calc as exec_owner
, f.name as financial_impact
, s.name as strategic_importance
, i.LastUpdateDate as modified_at
, updater.Fullname_calc as modified_by
, case when isnull((select Value from app.GlobalSiteSettings where Name = 'initiatives_search_visibility'),'N') = 'N' then 'N' else 'Y' end as visible
, STUFF((select '~|~' +  p.name from app.Collection p where i.DataInitiativeID=p.datainitiativeid and isnull(Hidden,'N')='N' FOR XML PATH('')), 1, 3, '') collection_name
, STUFF((select '~|~' +  p.Purpose + '~|~' + p.description from app.Collection p where i.DataInitiativeID=p.datainitiativeid and isnull(Hidden,'N')='N' FOR XML PATH('')), 1, 3, '') collection_description

, STUFF((select '~|~' +  t.name from app.Collection p inner join app.CollectionTerm a on p.DataProjectID = a.DataProjectId inner join app.term t on a.TermId=t.TermId where i.DataInitiativeID=p.datainitiativeid and isnull(Hidden,'N')='N' FOR XML PATH('')), 1, 3, '') term_name
, STUFF((select '~|~' +  t.Summary + '~|~' + t.TechnicalDefinition from app.Collection p inner join app.CollectionTerm a on p.DataProjectID = a.DataProjectId inner join app.term t on a.TermId=t.TermId where i.DataInitiativeID=p.datainitiativeid and isnull(Hidden,'N')='N' FOR XML PATH('')), 1, 3, '') term_description

, STUFF((select '~|~' +  t.name from app.Collection p inner join app.CollectionReport a on p.DataProjectID = a.DataProjectId inner join dbo.ReportObject t on a.ReportId = t.ReportObjectID left outer join app.reportobject_doc d on t.reportobjectid = d.reportobjectid where i.DataInitiativeID=p.datainitiativeid and isnull(p.Hidden,'N')='N' and isnull(d.Hidden,'N') = 'N' FOR XML PATH('')), 1, 3, '') report_name
, STUFF((select '~|~' +  t.DisplayTitle from app.Collection p inner join app.CollectionReport a on p.DataProjectID = a.DataProjectId inner join dbo.ReportObject t on a.ReportId = t.ReportObjectID left outer join app.reportobject_doc d on d.reportobjectid = t.reportobjectid where i.DataInitiativeID=p.datainitiativeid and isnull(p.Hidden,'N')='N' and displaytitle <> NULL and isnull(d.Hidden,'N') = 'N' FOR XML PATH('')), 1, 3, '') linked_name
, STUFF((select '~|~' +  r.Description + '~|~' + r.DetailedDescription + '~|~' + r.RepositoryDescription + '~|~' + d.DeveloperDescription + '~|~' + d.KeyAssumptions from app.Collection p inner join app.CollectionReport a on p.DataProjectID = a.DataProjectId inner join dbo.ReportObject r on r.ReportObjectID = a.ReportId left outer join app.ReportObject_doc d on r.ReportObjectID = d.ReportObjectID where i.DataInitiativeID=p.datainitiativeid and isnull(p.Hidden,'N')='N' and r.DefaultVisibilityYN = 'Y' and isnull(d.Hidden,'N') = 'N' FOR XML PATH('')), 1, 3, '') report_description

from app.Initiative i
left outer join dbo.[User] ops_owner on i.OperationOwnerID = ops_owner.UserId
left outer join dbo.[User] exec_owner on i.OperationOwnerID = exec_owner.UserId
left outer join app.FinancialImpact f on i.FinancialImpact = f.FinancialImpactId
left outer join app.StrategicImportance s on s.StrategicImportanceId = i.StrategicImportance
left outer join dbo.[User] updater on updater.UserId = i.LastUpdateUser
"""
)

columns = [column[0] for column in cursor.description]

batch_loader = partial(solr_load_batch, build_doc, SOLRURL)
list(map(batch_loader, rows(cursor, columns)))

cnxn.close()
