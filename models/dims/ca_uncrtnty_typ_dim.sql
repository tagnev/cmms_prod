{{
    config(
        materialized='incremental'
    )
}}

with sq_lookup_9 as (
select B.MSRMNT_RESLT_AND_DATA_ID,
B.MSRMNT_DATA_TEST_COND,
B.MSRMNT_DATA_COND_2,
B.MSRMNT_DATA_DATA_ID,
B.MSRMNT_DATA_SHORT_DATA_ID,
B.TEST_DATA_SCTN_ID,
B.UNT_ID,
B.CAL_DT,
B.SRVC_PRVDR_ID,
B.CAL_PROCEDURE_ID,
B.MSRMNT_UNIQUE_ID,
B.CXKEY_SRVC_ORD_NUM,
B.LAST_MODIFIED_DT,
B.CXKEY_CAL_PROCEDURE_ID,
B.CXKEY_MFGR_ID,
B.CXKEY_MFGR_MDL_NUM,
B.CXKEY_SR_NUM,
B.CXKEY_FRMWRK_ID,
B.CXKEY_SUBMITTING_ENTITY,
B.CXKEY_APP_PLATFORM,
B.CXKEY_APP_ID,
B.CXKEY_CAL_TEST_RESLT_NM,
B.CXKEY_TEST_DATA_SCTN_NM,
B.CXKEY_CAL_TEST_RESLT_TYP,
B.ASRECDORCOMP_DATA_IND,
B.MSRMNT_DATA_PRCSS_GDBD_ADJ_VAL,
B.MSRMNT_DTA_PRCSS_GDBD_ADJ_VL_T,
B.CXKEY_MSRMNT_ID
from {{ ref('ca_uncrtnty_typ_stg') }} A inner join {{ ref('ca_msrmnt_reslt_and_data_fact') }} B
on 
a.CXKEY_SRVC_ORD_NUM=B.CXKEY_SRVC_ORD_NUM and A.CXKEY_CAL_PROCEDURE_ID=B.CXKEY_CAL_PROCEDURE_ID and A.LAST_MODIFIED_DT=B.LAST_MODIFIED_DT 
and A.CXKEY_MFGR_ID=B.CXKEY_MFGR_ID and A.CXKEY_MFGR_MDL_NUM=B.CXKEY_MFGR_MDL_NUM AND A.CXKEY_SR_NUM=B.CXKEY_SR_NUM and A.CXKEY_FRMWRK_ID=B.CXKEY_FRMWRK_ID 
and A.CXKEY_SUBMITTING_ENTITY=B.CXKEY_SUBMITTING_ENTITY and A.CXKEY_APP_PLATFORM=B.CXKEY_APP_PLATFORM and A.CXKEY_APP_ID=B.CXKEY_APP_ID 
and A.CXKEY_CAL_TEST_RESLT_NM=B.CXKEY_CAL_TEST_RESLT_NM and A.CXKEY_TEST_DATA_SCTN_NM=B.CXKEY_TEST_DATA_SCTN_NM and A.CXKEY_CAL_TEST_RESLT_TYP=B.CXKEY_CAL_TEST_RESLT_TYP 
and A.CXKEY_MSRMNT_ID=B.CXKEY_MSRMNT_ID and A.SHORT_DATA_ID=B.msrmnt_data_short_data_id and A.condition_2=B.msrmnt_data_cond_2 and A.test_condition=B.msrmnt_data_test_cond 
),

final as (
  
select
    tgt.MSRMNT_RESLT_AND_DATA_ID,
	stg.CXKEY_MSRMNT_ID,
	stg.UNCRTNTY,
	stg.TYP,
	stg.IS_APPLD_UNCRTNTY,
	stg.IS_APPLD_PRCSS_UNCRTNTY,
	stg.NM,
	stg.UNT,
	stg.METROLOGY_DATABASE_NM,
	stg.CXKEY_SRVC_ORD_NUM,
	stg.LAST_MODIFIED_DT,
	stg.CXKEY_CAL_PROCEDURE_ID,
	stg.CXKEY_MFGR_ID,
	stg.CXKEY_MFGR_MDL_NUM,
	stg.CXKEY_SR_NUM,
	stg.CXKEY_FRMWRK_ID,
	stg.CXKEY_SUBMITTING_ENTITY,
	stg.CXKEY_CAL_TEST_RESLT_NM,
	stg.CXKEY_CAL_TEST_RESLT_TYP,
	stg.CXKEY_APP_PLATFORM,
	stg.CXKEY_APP_ID,
	stg.CXKEY_TEST_DATA_SCTN_NM,
	stg.CXKEY_TEST_DATA_SCTN_UNIQUE_ID,
	stg.TEST_CONDITION,
	stg.CONDITION_2,
	stg.SHORT_DATA_ID
    from {{ ref('ca_uncrtnty_typ_stg') }} stg
    left outer join sq_lookup_9 tgt
    on stg.CXKEY_SRVC_ORD_NUM = tgt.CXKEY_SRVC_ORD_NUM
)
select distinct
(SEQ_MSRMNT_RESLT_UNCRTNTY_DIM.nextval) as CA_UNCRTNTY_TYP_ID,
MSRMNT_RESLT_AND_DATA_ID,
CXKEY_MSRMNT_ID,
UNCRTNTY,
TYP,
IS_APPLD_UNCRTNTY,
IS_APPLD_PRCSS_UNCRTNTY,
NM,
UNT,
METROLOGY_DATABASE_NM,
CXKEY_SRVC_ORD_NUM,
LAST_MODIFIED_DT,
CXKEY_CAL_PROCEDURE_ID,
CXKEY_MFGR_ID,
CXKEY_MFGR_MDL_NUM,
CXKEY_SR_NUM,
CXKEY_FRMWRK_ID,
CXKEY_SUBMITTING_ENTITY,
CXKEY_CAL_TEST_RESLT_NM,
CXKEY_CAL_TEST_RESLT_TYP,
CXKEY_APP_PLATFORM,
CXKEY_APP_ID,
CXKEY_TEST_DATA_SCTN_NM,
CXKEY_TEST_DATA_SCTN_UNIQUE_ID,
TEST_CONDITION,
CONDITION_2,
SHORT_DATA_ID
from final
where MSRMNT_RESLT_AND_DATA_ID IS NOT NULL