{{
    config(
        materialized='incremental',
        unique_key=['CXKEY_SRVC_ORD_NUM',
        'LAST_MODIFIED_DT',
        'CXKEY_MFGR_ID',
        'CXKEY_MFGR_MDL_NUM',		
        'CXKEY_SR_NUM',
        'CXKEY_FRMWRK_ID',
        'CXKEY_SUBMITTING_ENTITY',
        'EQPMNT_ID','CAL_RESLT_ID','TEST_AFFCTD_OR_TEST_USED_NM','STD_USED_OR_ACCSSRY_FLAG','TRC_NUM','CAL_DUE_DT','RESULT_TYPE' ]
    )
}}

with ca_cal_reslt_std_acc_used_raw_stg
as
(
  select *
  from ca_cal_reslt_std_acc_used_raw
),
exp_std_testaffected
as 
(
  select 
"ManufID",
"ManufModel",
"SerialNumber",
IFF("TraceNumber" is null,'NO STD TRACE NUMBER',"TraceNumber") as "TraceNumber",
"CalDueDate",
"TestAffectedOrUsed",
"ResultsType",
"Role",
"SetupName",
"AffectedTraceParameters",
"STD_OR_ACC",
"CXKEY_ProcedureID",
"CXKEY_ManufID",
"CXKEY_ManufModel",
"CXKEY_SerialNumber",
"CXKEY_AppFramework_FrameworkID",
"CXKEY_SubmittingEntity",
"CXKEY_LastModified",
"CXKEY_ServiceOrderNumber"
  from 
ca_cal_reslt_std_acc_used_raw_stg
  where 
  STD_OR_ACC='STD'
),
exp_acc_testused
as
(
    select 
"ManufID",
"ManufModel",
"SerialNumber",
'NO ACC TRACE NUMBER' as "TraceNumber",
 TO_DATE('12-31-9999','MM-DD-YYYY') as "CalDueDate",
"TestAffectedOrUsed",
"ResultsType",
"Role",
"SetupName",
 NULL as "AffectedTraceParameters",
"STD_OR_ACC",
"CXKEY_ProcedureID",
"CXKEY_ManufID",
"CXKEY_ManufModel",
"CXKEY_SerialNumber",
"CXKEY_AppFramework_FrameworkID",
"CXKEY_SubmittingEntity",
"CXKEY_LastModified",
"CXKEY_ServiceOrderNumber"
  from 
ca_cal_reslt_std_acc_used_raw_stg
  where 
  STD_OR_ACC='ACC'
),
union_trans 
as
(
  select * from exp_std_testaffected
  union all
  select * from exp_acc_testused
),
lkptrans
as
(
  select stg.EQPMNT_ID,
  raw_stg.*
  from union_trans raw_stg
  left outer join
  CA_EQPMNT_STG stg
  on 
  stg.MFGR_ID = raw_stg."ManufID" AND 
  stg.MFGR_MDL_NUM = raw_stg."ManufModel"  AND 
  stg.SR_NUM = raw_stg."SerialNumber"  AND 
  stg.TRC_NUM = raw_stg."TraceNumber" AND 
  stg.CAL_DUE_DT = raw_stg."CalDueDate"
),
lkptrans1
as
(
   select stg.CAL_RESLT_ID, raw_stg.*
  from lkptrans raw_stg
  left outer join
  ca_cal_reslt_stg stg
  on
  stg.CXKEY_SRVC_ORD_NUM = raw_stg."CXKEY_ServiceOrderNumber" 
  AND stg.LAST_MODIFIED_DT = raw_stg."CXKEY_LastModified" 
  AND stg.CXKEY_CAL_PROCEDURE_ID = raw_stg."CXKEY_ProcedureID" 
  AND stg.CXKEY_MFGR_ID = raw_stg."CXKEY_ManufID" 
  AND stg.CXKEY_MFGR_MDL_NUM= raw_stg."CXKEY_ManufModel"
  AND stg.CXKEY_SR_NUM = raw_stg."CXKEY_SerialNumber" 
  AND stg.CXKEY_FRMWRK_ID = raw_stg."CXKEY_AppFramework_FrameworkID" 
  AND stg.CXKEY_SUBMITTING_ENTITY = raw_stg."CXKEY_SubmittingEntity"  
)
select distinct EQPMNT_ID as EQPMNT_ID,
CAL_RESLT_ID as CAL_RESLT_ID,
NULL as INSERT_DT,
NULL as UPD_DT,
"TestAffectedOrUsed" as TEST_AFFCTD_OR_TEST_USED_NM,
STD_OR_ACC as STD_USED_OR_ACCSSRY_FLAG,
"SetupName" as SETUP_NM,
"Role" as ROLE,
"CXKEY_ServiceOrderNumber" as CXKEY_SRVC_ORD_NUM,
"CXKEY_LastModified" as LAST_MODIFIED_DT,
"CXKEY_ProcedureID" as CXKEY_PROCEDURE_ID,
"CXKEY_ManufID" as CXKEY_MFGR_ID,
"CXKEY_ManufModel" as CXKEY_MFGR_MDL_NUM,
"CXKEY_SerialNumber" as CXKEY_SR_NUM,
"CXKEY_AppFramework_FrameworkID" as CXKEY_FRMWRK_ID,
"CXKEY_SubmittingEntity" as CXKEY_SUBMITTING_ENTITY,
"ResultsType" as RESULT_TYPE,
"AffectedTraceParameters" as AFFECTED_TRACE_PARAMETERS,
"ManufID" as STD_USED_OR_ACCSSRY_MFGR_ID,
"SerialNumber" as STD_USED_OR_ACCSSRY_SR_NUM,
"ManufModel" as STD_USED_OR_ACC_MFGR_MDL_NUM,
"TraceNumber" as TRC_NUM,
"CalDueDate" as CAL_DUE_DT from lkptrans1