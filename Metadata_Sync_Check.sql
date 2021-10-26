-----------------------------------------------------------------------------------------------
--CHECK SYNC FOR SOURCE TABLES
with cte_table_metadata as( --gives list of system,environments and tables available in the metadata manager
select SCE.SYSTEM_NAME,SCE.SYSTEM_ENVIRONMENT_NAME,ST.TABLE_NAME 
from SYSTEM_TABLE_METADATA ST 
inner join SYS_CON_ENVIRONMENTS SCE on ST.SCE_ID=SCE.SCE_ID and ST.SYSTEM_ID=SCE.SYSTEM_ID
where ST.STATUS='Active' and SCE.STATUS='Active'
),
cte_mapping_details as( --union of rows from appended rows and individual rows, extracting only active mappings where src and tgt columns are not null
select P.PROJ_NAME,A.* from (
select AP.SRC_SYSTEM_NAME,AP.SRC_SYSTEM_ENVIRONMENT_NAME,AP.TGT_SYSTEM_NAME,AP.TGT_SYSTEM_ENVIRONMENT_NAME,MD.MAP_ID,MD.MAP_NAME,MD.PROJECT_ID,AP.SRC_TABLE_NAME,AP.SRC_COLUMN_NAME,AP.TGT_TABLE_NAME,AP.TGT_COLUMN_NAME from APPENDED_MAP_SPEC_RECORDS AP inner join MAPPING_DETAILS MD on AP.MAP_ID=MD.MAP_ID where MD.STATUS='Active' and ((AP.SRC_COLUMN_NAME is not null and AP.SRC_COLUMN_NAME<>'') AND (AP.TGT_COLUMN_NAME is not null and AP.TGT_COLUMN_NAME<>''))
union 
select MS.SRC_SYSTEM_NAME,MS.SRC_SYSTEM_ENVIRONMENT_NAME,MS.TGT_SYSTEM_NAME,MS.TGT_SYSTEM_ENVIRONMENT_NAME,MD.MAP_ID,MD.MAP_NAME,MD.PROJECT_ID,MS.SRC_TABLE_NAME,MS.SRC_COLUMN_NAME,MS.TGT_TABLE_NAME,MS.TGT_COLUMN_NAME from MAPPING_SPECIFICATION MS inner join MAPPING_DETAILS MD on MS.MAP_ID=MD.MAP_ID where MD.STATUS='Active' and ((MS.SRC_COLUMN_NAME is not null and MS.SRC_COLUMN_NAME<>'') AND (MS.TGT_COLUMN_NAME is not null and MS.TGT_COLUMN_NAME<>'')) 
--and MS.MAP_SEQ_ID not in (select MAP_SEQ_ID from APPENDED_MAP_SPEC_RECORDS) 
and MS.SRC_COLUMN_NAME not like '%'+CHAR(10)+'%'
)A inner join PROJECT P on A.PROJECT_ID=P.PROJ_ID
)

select distinct PROJ_NAME,MAP_NAME,SRC_TABLE_NAME from ( --use this line to get only project and mapping names, inner query gives column level information
select 
m.PROJ_NAME,
m.MAP_ID,
m.MAP_NAME,
m.SRC_SYSTEM_NAME,
m.SRC_SYSTEM_ENVIRONMENT_NAME,
m.SRC_TABLE_NAME,
m.SRC_COLUMN_NAME,
m.TGT_SYSTEM_NAME,
m.TGT_SYSTEM_ENVIRONMENT_NAME,
m.TGT_TABLE_NAME,
m.TGT_COLUMN_NAME,
t.TABLE_NAME
from cte_mapping_details m Left join cte_table_metadata t on
t.table_name=m.SRC_TABLE_NAME and
t.SYSTEM_NAME = m.SRC_SYSTEM_NAME and
t.SYSTEM_ENVIRONMENT_NAME = m.SRC_SYSTEM_ENVIRONMENT_NAME
where 1=1
--and t.table_name is null --non-matched records only, sync failed
and t.table_name is not null --matched records only, sync successful
and SRC_TABLE_NAME like '%.%' 
  and SRC_TABLE_NAME not like '%#%'
  and SRC_TABLE_NAME not like '%result_of_%' 
  and SRC_TABLE_NAME not like '%RS_%'
  and SRC_TABLE_NAME not like '%MERGE-%'
  and SRC_TABLE_NAME not like '%INSERT-%'
  and SRC_TABLE_NAME not like '%UPDATE-%'

)Z;



-----------------------------------------------------------------------------------------------
--CHECK SYNC FOR TARGET TABLES
with cte_table_metadata as( --gives list of system,environments and tables available in the metadata manager
select SCE.SYSTEM_NAME,SCE.SYSTEM_ENVIRONMENT_NAME,ST.TABLE_NAME 
from SYSTEM_TABLE_METADATA ST 
inner join SYS_CON_ENVIRONMENTS SCE on ST.SCE_ID=SCE.SCE_ID and ST.SYSTEM_ID=SCE.SYSTEM_ID
where ST.STATUS='Active' and SCE.STATUS='Active'
),
cte_mapping_details as( --union of rows from appended rows and individual rows, extracting only active mappings where src and tgt columns are not null
select P.PROJ_NAME,A.* from (
select AP.SRC_SYSTEM_NAME,AP.SRC_SYSTEM_ENVIRONMENT_NAME,AP.TGT_SYSTEM_NAME,AP.TGT_SYSTEM_ENVIRONMENT_NAME,MD.MAP_ID,MD.MAP_NAME,MD.PROJECT_ID,AP.SRC_TABLE_NAME,AP.SRC_COLUMN_NAME,AP.TGT_TABLE_NAME,AP.TGT_COLUMN_NAME from APPENDED_MAP_SPEC_RECORDS AP inner join MAPPING_DETAILS MD on AP.MAP_ID=MD.MAP_ID where MD.STATUS='Active' and ((AP.SRC_COLUMN_NAME is not null and AP.SRC_COLUMN_NAME<>'') AND (AP.TGT_COLUMN_NAME is not null and AP.TGT_COLUMN_NAME<>''))
union 
select MS.SRC_SYSTEM_NAME,MS.SRC_SYSTEM_ENVIRONMENT_NAME,MS.TGT_SYSTEM_NAME,MS.TGT_SYSTEM_ENVIRONMENT_NAME,MD.MAP_ID,MD.MAP_NAME,MD.PROJECT_ID,MS.SRC_TABLE_NAME,MS.SRC_COLUMN_NAME,MS.TGT_TABLE_NAME,MS.TGT_COLUMN_NAME from MAPPING_SPECIFICATION MS inner join MAPPING_DETAILS MD on MS.MAP_ID=MD.MAP_ID where MD.STATUS='Active' and ((MS.SRC_COLUMN_NAME is not null and MS.SRC_COLUMN_NAME<>'') AND (MS.TGT_COLUMN_NAME is not null and MS.TGT_COLUMN_NAME<>'')) 
--and MS.MAP_SEQ_ID not in (select MAP_SEQ_ID from APPENDED_MAP_SPEC_RECORDS) 
and MS.SRC_COLUMN_NAME not like '%'+CHAR(10)+'%'
)A inner join PROJECT P on A.PROJECT_ID=P.PROJ_ID
)
--select distinct PROJ_NAME,MAP_NAME,TGT_TABLE_NAME from ( --use this line to get only project and mapping names, inner query gives column level information
select 
m.PROJ_NAME,
m.MAP_ID,
m.MAP_NAME,
m.SRC_SYSTEM_NAME,
m.SRC_SYSTEM_ENVIRONMENT_NAME,
m.SRC_TABLE_NAME,
m.SRC_COLUMN_NAME,
m.TGT_SYSTEM_NAME,
m.TGT_SYSTEM_ENVIRONMENT_NAME,
m.TGT_TABLE_NAME,
m.TGT_COLUMN_NAME,
t.TABLE_NAME
from cte_mapping_details m Left join cte_table_metadata t on
t.table_name=m.TGT_TABLE_NAME and
t.SYSTEM_NAME = m.TGT_SYSTEM_NAME and
t.SYSTEM_ENVIRONMENT_NAME = m.TGT_SYSTEM_ENVIRONMENT_NAME
where 1=1
--and t.table_name is null --non-matched records only, sync failed
and t.table_name is not null --matched records only, sync successful
and TGT_TABLE_NAME like '%.%' 
  --and TGT_TABLE_NAME not like '%#%'
  --and TGT_TABLE_NAME not like '%result_of_%' 
  --and TGT_TABLE_NAME not like '%RS_%'
  --and TGT_TABLE_NAME not like '%MERGE-%'
  --and TGT_TABLE_NAME not like '%INSERT-%'
  --and TGT_TABLE_NAME not like '%UPDATE-%'

--)Z

