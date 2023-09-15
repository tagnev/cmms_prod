{{
    config(
        materialized='incremental',
        unique_key=['CXKEY_SRVC_ORD_NUM',
        'LAST_MODIFIED_DT',
        'CXKEY_CAL_PROCEDURE_ID',
        'CXKEY_MFGR_ID',
        'CXKEY_MFGR_MDL_NUM',		
        'CXKEY_SR_NUM',
        'CXKEY_FRMWRK_ID',
        'CXKEY_SUBMITTING_ENTITY' ]
    )
}}

with ca_cal_reslt_raw_stg
as
(
  select 
  "CalDate",
  "AsReceivedCondition",
  "AsCompletedCondition",
  "ResponsiblePersonID",
  "LineMainsFrequency",
  "TemperatureRange_Actual",
  "TemperatureRange_Nominal",
  "TemperatureRange_Tolerance",
  "HumidityRange_Actual",
  "HumidityRange_Nominal",
  "HumidityRange_Tolerance",
  "LastAdjustmentDate",
  "SealsIntactFlag",
  "HumidityRange_Minimum",
  "HumidityRange_Maximum",
  "TemperatureRange_CustomerEnvironment",
  "HumidityRange_CustomerEnvironment",
  "CorrectiveAction",
  "ProcedureID",
  "ManufID",
  "ManufModel",
  "SerialNumber",
  "AppFramework_FrameworkID",
  "LastModified",
  "SubmittingEntity",
  "ServiceOrderNumber"
  from ca_cal_reslt_raw
),
lkptrans as
(
  select  distinct
  cal_reslt_data_id,
  raw_stg."CalDate",
  raw_stg."AsReceivedCondition",
  raw_stg."AsCompletedCondition",
  raw_stg."ResponsiblePersonID",
  raw_stg."LineMainsFrequency",
  raw_stg."TemperatureRange_Actual",
  raw_stg."TemperatureRange_Nominal",
  raw_stg."TemperatureRange_Tolerance",
  raw_stg."HumidityRange_Actual",
  raw_stg."HumidityRange_Nominal",
  raw_stg."HumidityRange_Tolerance",
  raw_stg."LastAdjustmentDate",
  raw_stg."SealsIntactFlag",
  raw_stg."HumidityRange_Minimum",
  raw_stg."HumidityRange_Maximum",
  raw_stg."TemperatureRange_CustomerEnvironment",
  raw_stg."HumidityRange_CustomerEnvironment",
  raw_stg."CorrectiveAction",
  raw_stg."ProcedureID",
  raw_stg."ManufID",
  raw_stg."ManufModel",
  raw_stg."SerialNumber",
  raw_stg."AppFramework_FrameworkID",
  raw_stg."LastModified",
  raw_stg."SubmittingEntity",
  raw_stg."ServiceOrderNumber"
  from 
  ca_cal_reslt_raw_stg raw_stg
  left outer join 
  CA_CAL_RESLT_DATA_STG stg
  on
  stg.CXKEY_SRVC_ORD_NUM = raw_stg."ServiceOrderNumber" 
  AND stg.LAST_MODIFIED_DT = raw_stg."LastModified" 
  AND stg.CXKEY_CAL_PROCEDURE_ID = raw_stg."ProcedureID" 
  AND stg.CXKEY_MFGR_ID = raw_stg."ManufID" 
  AND stg.CXKEY_MFGR_MDL_NUM = raw_stg."ManufModel" 
  AND stg.CXKEY_SR_NUM = raw_stg."SerialNumber" 
  AND stg.CXKEY_FRMWRK_ID = raw_stg."AppFramework_FrameworkID" 
  AND stg.CXKEY_SUBMITTING_ENTITY = raw_stg."SubmittingEntity"
),
final as 
(
   select distinct (CA_CAL_RESLT_STG_SEQ.nextval) as cal_reslt_id,
  "AsCompletedCondition" as AS_CPMPLT_COND,
  "AsReceivedCondition" as AS_RECVD_COND,
  "CalDate" as CAL_DT,
   cal_reslt_data_id as CAL_RESLT_DATA_ID,
  "CorrectiveAction" as CORRCTV_ACTION_VAL,
  "ProcedureID" as CXKEY_CAL_PROCEDURE_ID,
  "AppFramework_FrameworkID" as CXKEY_FRMWRK_ID,
  "ManufID" as CXKEY_MFGR_ID,
  "ManufModel" as CXKEY_MFGR_MDL_NUM,
  "SerialNumber" as CXKEY_SR_NUM,
  "ServiceOrderNumber" as CXKEY_SRVC_ORD_NUM,
  "SubmittingEntity" as CXKEY_SUBMITTING_ENTITY,
  "HumidityRange_Actual" as HUMDTY_RNG_ACT,
  "HumidityRange_CustomerEnvironment" as HUMDTY_RNG_CUST_ENV_IND,
  "HumidityRange_Maximum" as HUMDTY_RNG_MAX,
  "HumidityRange_Minimum" as HUMDTY_RNG_MIN,
  "HumidityRange_Nominal" as HUMDTY_RNG_NOMINAL,
  "HumidityRange_Tolerance" as HUMDTY_RNG_TOLERANCE,
  CURRENT_DATE() as insert_dt,
  TO_DATE('12-31-9999', 'mm-dd-yyyy' ) as upd_dt ,
  "LastAdjustmentDate" as LAST_ADJ_DT,
  "LastModified" as LAST_MODIFIED_DT,
  "LineMainsFrequency" as LINE_MAINS_FREQ_VAL,
  "ResponsiblePersonID" as RESTRICT_PER_ID_STR,
  "SealsIntactFlag" as SEAL_INTACT_FLAG,
  "TemperatureRange_Actual" as TMP_RNG_ACT,
  "TemperatureRange_CustomerEnvironment" as TMP_RNG_CUST_ENV_IND,
  "TemperatureRange_Nominal" as TMP_RNG_NOMINAL,
  "TemperatureRange_Tolerance" as TMP_RNG_TOLERANCE
  from lkptrans
)
select * from final