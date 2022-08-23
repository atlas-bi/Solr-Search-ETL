"""Atlas Solr ETL for Collections."""
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


def build_doc(collection: SimpleNamespace) -> Dict:
    """Build a collection doc."""
    doc = {
        "id": "/collections?id=%s" % collection.collection_id,
        "atlas_id": collection.collection_id,
        "type": "collections",
        "name": collection.name,
        "visible": collection.visible,
        "orphan": "N",
        "runs": 10,
        "description": [
            clean_description(collection.search_summary),
            clean_description(collection.description),
        ],
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
            [
                clean_description(x)
                for x in collection.initiative_description.split("~|~")
                if x
            ]
            if collection.initiative_description
            else []
        )
        + (
            [
                clean_description(x)
                for x in collection.term_description.split("~|~")
                if x
            ]
            if collection.term_description
            else []
        )
        + (
            [
                clean_description(x)
                for x in collection.linked_description.split("~|~")
                if x
            ]
            if collection.linked_description
            else []
        ),
    }

    return clean_doc(doc)


cnxn, cursor = connect()

cursor.execute(
    """select
  p.collectionid as collection_id
, p.name as name
, p.Purpose as search_summary
, p.Description as description
, p.LastUpdateDate as modified_at
, updater.Fullname_calc as modified_by
, case when isnull((select Value from app.GlobalSiteSettings where Name = 'collections_search_visibility'),'N') = 'N' or Hidden='Y' then 'N' else 'Y' end as visible

, STUFF((select '~|~' +  i.name from app.Initiative i where i.InitiativeID=p.initiativeid and isnull(i.Hidden,'N')='N' FOR XML PATH('')), 1, 3, '') initiative_name
, STUFF((select '~|~' +  i.description from app.Initiative i where i.InitiativeID=p.initiativeid and isnull(i.Hidden,'N')='N' FOR XML PATH('')), 1, 3, '') initiative_description


, STUFF((select '~|~' +  t.name from app.CollectionTerm a inner join app.term t on a.TermId = t.termid where a.collectionid=p.collectionid  FOR XML PATH('')), 1, 3, '') term_name
, STUFF((select '~|~' +  t.Summary + '~|~' + t.TechnicalDefinition from app.CollectionTerm a inner join app.term t on a.termid = t.termid where a.collectionid=p.collectionid FOR XML PATH('')), 1, 3, '')term_description



, STUFF((select '~|~' +  r.name from app.CollectionReport a inner join dbo.ReportObject r on a.ReportId = r.ReportObjectID left outer join app.reportobject_doc d on r.reportobjectid = d.reportobjectid  where a.collectionid=p.collectionid and isnull(d.Hidden,'N') = 'N' FOR XML PATH('')), 1, 3, '') report_name
, STUFF((select '~|~' +  r.DisplayTitle from app.CollectionReport a inner join dbo.ReportObject r on a.ReportId = r.ReportObjectID left outer join app.reportobject_doc d on r.reportobjectid = d.reportobjectid where a.collectionid=p.collectionid and r.DisplayTitle <> NULL and isnull(d.Hidden,'N') = 'N' FOR XML PATH('')), 1, 3, '') linked_name
, STUFF((select '~|~' +  r.Description + '~|~' + r.DetailedDescription + '~|~' + r.RepositoryDescription + '~|~' + d.DeveloperDescription + '~|~' + d.KeyAssumptions from app.CollectionReport a inner join dbo.ReportObject r on a.ReportId = r.ReportObjectID left outer join app.ReportObject_doc d on r.ReportObjectID = d.ReportObjectID where a.collectionid=p.collectionid and isnull(d.Hidden,'N') = 'N' FOR XML PATH('')), 1, 3, '') linked_description


from app.Collection p
left outer join dbo.[User] updater on p.LastUpdateUser = updater.UserId
"""
)

columns = [column[0] for column in cursor.description]

batch_loader = partial(solr_load_batch, build_doc, SOLRURL)
list(map(batch_loader, rows(cursor, columns)))

cnxn.close()
