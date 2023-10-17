{{
    config(
        materialized='incremental',
        unique_key=['CXKEY_SRVC_ORD_NUM',
        'CXKEY_CAL_PROCEDURE_ID',
        'CXKEY_MFGR_ID',
        'CXKEY_MFGR_MDL_NUM',		
        'CXKEY_SR_NUM',
        'CXKEY_FRMWRK_ID',
        'CXKEY_SUBMITTING_ENTITY']
    )
}}


with ca_cal_srvc_reslt_raw_stg 
as
(
  select * from ca_cal_srvc_reslt_raw
),
lkptrans_cal_procedure_stg
as
(
  select distinct stg.cal_procedure_id
  from ca_cal_srvc_reslt_raw raw_stg
  left outer join
  CA_CAL_PROCEDURE_STG stg
  on 
  stg.PROCEDURE_ID = raw_stg."ProcedureID" AND 
  stg.FRMWRK_ID = raw_stg."AppFramework_FrameworkID"  
 --stg.PROCEDURE_VER_NUM = raw_stg."Version"
 --AND stg.FRMWRK_VER_NUM = raw_stg."AppFramework_FrameworkID"
),
lkp_CA_CAL_RESLT_CONTXT_STG
as
(
  select distinct
  stg.CAL_RESLT_CONTXT_ID
  from ca_cal_srvc_reslt_raw raw_stg
  left outer join
  CA_CAL_RESLT_CONTXT_STG stg
  on
  stg.CXKEY_SRVC_ORD_NUM = raw_stg."ServiceOrderNumber" AND 
--   stg.LAST_MODIFIED_DT = raw_stg."LastModified" AND 
  stg.CXKEY_CAL_PROCEDURE_ID = raw_stg."ProcedureID" AND 
  stg.CXKEY_MFGR_ID = raw_stg."ManufID" AND 
  stg.CXKEY_MFGR_MDL_NUM = raw_stg."ManufModel" AND 
  stg.CXKEY_SR_NUM = raw_stg."SerialNumber" AND 
  stg.CXKEY_FRMWRK_ID = raw_stg."AppFramework_FrameworkID" AND 
  stg.CXKEY_SUBMITTING_ENTITY = raw_stg."SubmittingEntity"
),
lkp_CA_UNT_STG
as
(
  select distinct
  stg.UNT_ID
  from ca_cal_srvc_reslt_raw raw_stg
  left outer join
  CA_UNT_STG stg
  on
  stg.MFGR_MDL_NUM = raw_stg."ManufModel" AND 
  stg.MFGR_ID = raw_stg."ManufID" AND 
  stg.SR_NUM = raw_stg."SerialNumber"
),
lkp_ca_cal_reslt_stg
as 
(
  select distinct stg.CAL_RESLT_ID 
  from ca_cal_srvc_reslt_raw raw_stg
  left outer join
  ca_cal_reslt_stg stg
  on
  stg.CXKEY_SRVC_ORD_NUM = raw_stg."ServiceOrderNumber" 
--   AND stg.LAST_MODIFIED_DT = raw_stg."LastModified" 
  AND stg.CXKEY_CAL_PROCEDURE_ID = raw_stg."ProcedureID" 
  AND stg.CXKEY_MFGR_ID = raw_stg."ManufID" 
  AND stg.CXKEY_MFGR_MDL_NUM = raw_stg."ManufModel" 
  AND stg.CXKEY_SR_NUM = raw_stg."SerialNumber" 
  AND stg.CXKEY_FRMWRK_ID = raw_stg."AppFramework_FrameworkID" 
  AND stg.CXKEY_SUBMITTING_ENTITY = raw_stg."SubmittingEntity" 
),
exp 
as
(
    select distinct
  (select max(CAL_RESLT_ID) from lkp_ca_cal_reslt_stg) as CAL_RESLT_ID,
  (select max(UNT_ID) from lkp_CA_UNT_STG) as UNT_ID,
  (select max(CAL_RESLT_CONTXT_ID) from lkp_CA_CAL_RESLT_CONTXT_STG) as CAL_RESLT_CONTXT_ID,
  (select max(cal_procedure_id) from lkptrans_cal_procedure_stg) as cal_procedure_id,
  * from ca_cal_srvc_reslt_raw_stg
),
final as ( 
  select (CA_CAL_SRVC_RESLT_STG_SEQ.nextval) as CALIB_SRVC_RESLT_ID,
  *,
   NULL as insert_Dt,
  NULL as upd_dt
  from exp
  )
  select distinct
  "CustomerNumber" as CUST_ID,
"AccreditationID" as ACCRDTTN_ID,
"CalInterval" as CAL_INTRVL_MTS,
"Source" as CAL_INTRVL_SRC_NM,
CAL_PROCEDURE_ID as CAL_PROCEDURE_ID,
CAL_RESLT_CONTXT_ID as CAL_RESLT_CONTXT_ID,
CAL_RESLT_ID as CAL_RESLT_ID,
CALIB_SRVC_RESLT_ID as CALIB_SRVC_RESLT_ID,
"CalibrationType" as CALIB_TYP,
"Created" as CREATED_DT,
"CustomerName" as CUSTOMER_NAME,
"ProcedureID" as CXKEY_CAL_PROCEDURE_ID,
"AppFramework_FrameworkID" as CXKEY_FRMWRK_ID,
"ManufID" as CXKEY_MFGR_ID,
"ManufModel" as CXKEY_MFGR_MDL_NUM,
"SerialNumber" as CXKEY_SR_NUM,
"ServiceOrderNumber" as CXKEY_SRVC_ORD_NUM,
"SubmittingEntity" as CXKEY_SUBMITTING_ENTITY,
"EngComment" as ENG_COMMNT,
INSERT_DT as INSERT_DT,
"LastModified" as LAST_MODIFIED_DT,
"OCN" as OCN,
"OSN" as OSN,
"Platform" as PLATFORM,
"ReportNumber" as REPRT_NUM,
"ServiceDefinitionID" as SRVC_DEF_ID,
"AdjustmentValue" as SRVC_GURDBD_ADJ_VAL,
"Guardband_Type" as SRVC_GURDBD_ADJ_VAL_TYP,
"Value" as SRVC_GURDBD_VAL,
"Type" as SRVC_GURDBD_VAL_TYP,
"ServiceType" as SRVC_TYP,
"SubmittingEntity" as SUBMTTD_ENTY_NM,
"UncertaintyRequirement" as UNCRTNTY_RQRMNT_FLAG,
UNT_ID as UNT_ID,
UPD_DT as UPD_DT,
"Version" as VER_NUM
  from final