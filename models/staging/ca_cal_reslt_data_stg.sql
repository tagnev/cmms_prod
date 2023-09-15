{{
    config(
        materialized='incremental', unique_key=['CXKEY_SRVC_ORD_NUM', 'CXKEY_SR_NUM']
    )
}}

with
    ca_cal_reslt_data_raw_stg as (
      select 
        "AsReceivedReportedJudgement",
        "AsReceivedStandardJudgement",
        "AsReceivedGuardbandJudgement",
        "AsReceivedProcessJudgement",
        "AsReceivedNearSpecificationLimit",
        "AsReceivedNearGuardbandLimit",
        "AsCompletedReportedJudgement",
        "AsCompletedStandardJudgement",
        "AsCompletedGuardbandJudgement",
        "AsCompletedProcessJudgement",
        "AsCompletedNearSpecificationLimit",
        "AsCompletedNearGuardbandLimit",
        "AsReceivedFirmwareVersion",
        "ProcedureID",
        "ManufID",
        "ManufModel",
        "SerialNumber",
        "AppFramework_FrameworkID",
        "LastModified",
        "SubmittingEntity",
        "ServiceOrderNumber"
    from ca_cal_reslt_data_raw
    ),
    expression as (
        select distinct *,
        'X' as RT,
        IFF(RT = 'A','AS_REC_CMP',IFF(RT = 'B','BOTH',IFF(RT = 'C','AS_COMPLETED',IFF(RT =  'R','AS_RECEIVED','TYPE_X')))) as CAL_RESLT_TYP,
        RT as CAL_DATA_INDICATOR,
        IFF("AsReceivedReportedJudgement" = 'NOT_DONE' OR "AsReceivedReportedJudgement" = 'UNKNOWN' OR "AsReceivedReportedJudgement" is null OR "AsCompletedReportedJudgement" = 'NOT_DONE' OR "AsCompletedReportedJudgement" = 'UNKNOWN' OR "AsCompletedReportedJudgement" is null,'TRUE','FALSE') as RESLT_TYP_DATA_IS_COPY
        from ca_cal_reslt_data_raw_stg
    )
    select (CA_CAL_RESLT_DATA_STG_SEQ.nextval) as cal_reslt_data_id, 
        "AsReceivedReportedJudgement" as AS_RECVD_RPRTD_JDGMNT,
        "AsReceivedStandardJudgement" as AS_RECVD_STD_JDGMNT,
        "AsReceivedGuardbandJudgement" as AS_RECVD_GURDBD_JDGMNT,
        "AsReceivedProcessJudgement" as AS_RECVD_PRCSS_JDGMNT,
        "AsReceivedNearSpecificationLimit" as AS_RECVD_NEAR_SPCFCTN_LMT_IND,
        "AsReceivedNearGuardbandLimit" as AS_RECVD_NEAR_GURDBD_LMT_IND,
        "AsCompletedReportedJudgement" as AS_CPMPLT_RPRTD_JDGMNT,
        "AsCompletedStandardJudgement" as AS_CPMPLT_STD_JDGMNT,
        "AsCompletedGuardbandJudgement" as AS_CPMPLT_GURDBD_JDGMNT,
        "AsCompletedProcessJudgement" as AS_CPMPLT_PRCSS_JDGMNT,
        "AsCompletedNearSpecificationLimit" as AS_CPMPLT_NEAR_SPCFCTN_LMT_IND,
        "AsCompletedNearGuardbandLimit" as AS_CPMPLT_NEAR_GURDBD_LMT_IND,
        "AsReceivedFirmwareVersion" as AS_RECVD_FIRMWARE_VER_NUM,
        "ProcedureID" as CXKEY_CAL_PROCEDURE_ID,
        "ManufID" as CXKEY_MFGR_ID,
        "ManufModel" as CXKEY_MFGR_MDL_NUM,
        "SerialNumber" as CXKEY_SR_NUM,
        "AppFramework_FrameworkID" as CXKEY_FRMWRK_ID,
        "LastModified" as LAST_MODIFIED_DT,
        "SubmittingEntity" as CXKEY_SUBMITTING_ENTITY,
        "ServiceOrderNumber" as CXKEY_SRVC_ORD_NUM,
        RT,
        CAL_RESLT_TYP,
        CAL_DATA_INDICATOR,
        RESLT_TYP_DATA_IS_COPY,
        current_date() as insert_dt,
        to_date('12-31-9999', 'mm-dd-yyyy') as upd_dt,
        Null as AS_REC_RPTRD_JUDGMNT,
        Null as AS_CMPL_RPRTD_JUDGMNT
     from expression