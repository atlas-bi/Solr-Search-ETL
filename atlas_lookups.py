"""Load atlas lookups to solr search."""
from functools import partial
from types import SimpleNamespace
from typing import Dict

import settings
from functions import clean_doc, connect, rows, solr_load_batch


def build_doc(lookup: SimpleNamespace) -> Dict:
    """Build lookup doc."""
    doc = {
        "id": lookup.id,
        "atlas_id": lookup.atlas_id,
        "item_type": lookup.item_type,
        "item_name": lookup.item_name,
    }

    return clean_doc(doc)


cnxn, cursor = connect()

cursor.execute(
    """
select
concat('financial_impact_', cast(FinancialImpactId as nvarchar)) as id,
'financial_impact' as item_type,
Name as 'item_name',
FinancialImpactId as 'atlas_id'
from app.FinancialImpact

union all

select
concat('fragility_', cast(FragilityID as nvarchar)) as id,
'fragility' as item_type,
FragilityName as 'item_name',
FragilityID as 'atlas_id'
from app.Fragility

union all

select
concat('fragility_tag_', cast(FragilityTagID as nvarchar)) as id,
'fragility_tag' as item_type,
FragilityTagName as 'item_name',
FragilityTagID as 'atlas_id'
from app.FragilityTag

union all

select
concat('initiative_contacts_', cast(ContactID as nvarchar)) as id,
'initiative_contacts' as item_type,
Name as 'item_name',
ContactID as 'atlas_id'
from app.DP_Contact

union all

select
concat('maintenance_log_status_', cast(MaintenanceLogStatusID as nvarchar)) as id,
'maintenance_log_status' as item_type,
MaintenanceLogStatusName as 'item_name',
MaintenanceLogStatusID as 'atlas_id'
from app.MaintenanceLogStatus

union all

select
concat('maintenance_schedule_', cast(MaintenanceScheduleID as nvarchar)) as id,
'maintenance_schedule' as item_type,
MaintenanceScheduleName as 'item_name',
MaintenanceScheduleID as 'atlas_id'
from app.MaintenanceSchedule

union all

select
concat('organizational_value_', cast(OrganizationalValueId as nvarchar)) as id,
'organizational_value' as item_type,
OrganizationalValueName as 'item_name',
OrganizationalValueId as 'atlas_id'
from app.OrganizationalValue

union all

select
concat('run_frequency_', cast(EstimatedRunFrequencyId as nvarchar)) as id,
'run_frequency' as item_type,
EstimatedRunFrequencyName as 'item_name',
EstimatedRunFrequencyId as 'atlas_id'
from app.EstimatedRunFrequency

union all

select
concat('strategic_importance_', cast(StrategicImportanceId as nvarchar)) as id,
'strategic_importance' as item_type,
Name as 'item_name',
StrategicImportanceId as 'atlas_id'
from app.StrategicImportance

union all

select
concat('user_roles_', cast(UserRolesId as nvarchar)) as id,
'user_roles' as item_type,
Name as 'item_name',
UserRolesId as 'atlas_id'
from app.UserRoles
"""
)

columns = [column[0] for column in cursor.description]

batch_loader = partial(solr_load_batch, build_doc, settings.SOLR_LOOKUP_URL)
list(map(batch_loader, rows(cursor, columns)))

cnxn.close()
