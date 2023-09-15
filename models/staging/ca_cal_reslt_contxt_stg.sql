{{
    config(
        materialized='incremental',
        unique_key=['PRVDR_ENTY_CD', 'CXKEY_SR_NUM', 'CAL_RESLT_CONTXT_ID']
    )
}}

with ca_cal_reslt_contxt_raw_stg 
as
(
  select 
"ServiceProvider",
"ServiceAddress1",
"ServiceAddress2",
"ServiceAddress3",
"ProviderEntity",
"SubmittingEntity",
"ResponsibleEntity",
"CalHours",
-- "SystemCalHours",
"ProcedureID",
"ManufID",
"ManufModel",
"SerialNumber",
"AppFramework_FrameworkID",
"LastModified",
"ServiceOrderNumber"
  from ca_cal_reslt_contxt_raw
),
lkptrans as
(
  select stg.SRVC_PRVDR_ID,
  raw_stg."ProviderEntity",
  stg.PRVDR_ENTY_CD
  from ca_cal_reslt_contxt_raw_stg raw_stg
  left outer join
  CA_SRVC_PRVDR_STG stg
  on 
  raw_stg."ProviderEntity"=stg.PRVDR_ENTY_CD
),
exp as
(
  select
  (select  max(SRVC_PRVDR_ID) from lkptrans where "ProviderEntity"=PRVDR_ENTY_CD) as SRVC_PRVDR_ID,
  "ServiceAddress1",
  "ServiceAddress2",
  "ServiceAddress3",
  "ProviderEntity",
  LOWER("SubmittingEntity") AS "SubmittingEntity",
  "ResponsibleEntity",
  "CalHours",
--   "SystemCalHours",
  "ProcedureID",
  "ManufID",
  "ManufModel",
  "SerialNumber",
  "AppFramework_FrameworkID",
  "LastModified",
  "ServiceOrderNumber"
  from ca_cal_reslt_contxt_raw_stg
),
final as 
(
  select distinct (CA_CAL_RESLT_CONTXT_STG_SEQ.nextval) as cal_reslt_contxt_id,
  SRVC_PRVDR_ID,
  "ServiceAddress1" as ADDR_LINE_1,
  "ServiceAddress2" as ADDR_LINE_2,
  "ServiceAddress3" as ADDR_LINE_3,
  "ProviderEntity" as PRVDR_ENTY_CD,
  "SubmittingEntity" as CXKEY_SUBMITTING_ENTITY,
  "ResponsibleEntity" as RESPONSIBLE_ENTY_CD,
  "CalHours" as CAL_HOURS,
NULL as SYS_CAL_HOURS,
  "ProcedureID" as CXKEY_CAL_PROCEDURE_ID,
  "ManufID" as CXKEY_MFGR_ID,
  "ManufModel" as CXKEY_MFGR_MDL_NUM,
  "SerialNumber" as CXKEY_SR_NUM,
  "AppFramework_FrameworkID" as CXKEY_FRMWRK_ID,
  "LastModified" as LAST_MODIFIED_DT,
  "ServiceOrderNumber" as CXKEY_SRVC_ORD_NUM,
  NULL as insert_Dt,
  NULL as upd_dt
  from exp
)
select * from final