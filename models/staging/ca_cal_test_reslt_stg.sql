{{
    config(
        materialized='table',
    )
}}

with ca_cal_test_reslt_raw_stg as 
(
select 
CXKEY_SRVC_ORD_NUM,
LAST_MODIFIED_DT,
CXKEY_CAL_PROCEDURE_ID,
CXKEY_MFGR_ID,
CXKEY_MFGR_MDL_NUM,
CXKEY_SR_NUM,
CXKEY_FRMWRK_ID,
CXKEY_SUBMITTING_ENTITY,
ASRECDORCOMP_DATA_IND,
CXKEY_TEST_NM,
CXKEY_APP_PLATFORM,
CXKEY_APP_ID
from 
CA_ASRECDORCOMP_DATA_TYP_STG
  ),
  lkptrans
  as
  (
    select stg.CAL_RESLT_DATA_ID,raw_stg.CXKEY_SRVC_ORD_NUM 
    from 
    ca_cal_test_reslt_raw_stg raw_stg
    left outer join
    CA_CAL_RESLT_DATA_STG stg
    on 
    stg.CXKEY_SRVC_ORD_NUM 			= raw_stg.CXKEY_SRVC_ORD_NUM 		AND
stg.LAST_MODIFIED_DT 			= raw_stg.LAST_MODIFIED_DT 		    AND
stg.CXKEY_CAL_PROCEDURE_ID 		= raw_stg.CXKEY_CAL_PROCEDURE_ID    AND
stg.CXKEY_MFGR_ID 				= raw_stg.CXKEY_MFGR_ID 			AND
stg.CXKEY_MFGR_MDL_NUM 			= raw_stg.CXKEY_MFGR_MDL_NUM 		AND
stg.CXKEY_SR_NUM 				= raw_stg.CXKEY_SR_NUM 			    AND
stg.CXKEY_FRMWRK_ID 			= raw_stg.CXKEY_FRMWRK_ID 		    AND
stg.CXKEY_SUBMITTING_ENTITY 	= raw_stg.CXKEY_SUBMITTING_ENTITY   
    ),
    lkptrans1
    as
    (
      select 
      stg.CAL_TEST_DATA_TYP_ID as AS_CPMPLT_CAL_TEST_DATA_TYP_ID,raw_stg.CXKEY_SRVC_ORD_NUM ,raw_stg.CXKEY_TEST_NM,
      stg.ASRECDORCOMP_DATA_IND
      from 
       ca_cal_test_reslt_raw_stg raw_stg
    left outer join
    CA_ASRECDORCOMP_DATA_TYP_STG stg
      on 
      stg.CXKEY_TEST_NM =raw_stg.CXKEY_TEST_NM AND
          stg.CXKEY_SRVC_ORD_NUM 			= raw_stg.CXKEY_SRVC_ORD_NUM 		AND
stg.LAST_MODIFIED_DT 			= raw_stg.LAST_MODIFIED_DT 		    AND
stg.CXKEY_CAL_PROCEDURE_ID 		= raw_stg.CXKEY_CAL_PROCEDURE_ID    AND
stg.CXKEY_MFGR_ID 				= raw_stg.CXKEY_MFGR_ID 			AND
stg.CXKEY_MFGR_MDL_NUM 			= raw_stg.CXKEY_MFGR_MDL_NUM 		AND
stg.CXKEY_SR_NUM 				= raw_stg.CXKEY_SR_NUM 			    AND
stg.CXKEY_FRMWRK_ID 			= raw_stg.CXKEY_FRMWRK_ID 		    AND
stg.CXKEY_SUBMITTING_ENTITY 	= raw_stg.CXKEY_SUBMITTING_ENTITY   
    where  raw_stg.ASRECDORCOMP_DATA_IND='AS_COMPLETED'
    ),
    lkptrans2
    as
    (
      select 
      stg.CAL_TEST_DATA_TYP_ID as AS_RECVD_CAL_TEST_DATA_TYP_ID,raw_stg.CXKEY_SRVC_ORD_NUM ,raw_stg.CXKEY_TEST_NM,
      raw_stg.LAST_MODIFIED_DT,
      raw_stg.CXKEY_CAL_PROCEDURE_ID,
      raw_stg.CXKEY_MFGR_ID,
      raw_stg.CXKEY_MFGR_MDL_NUM,
      raw_stg.CXKEY_SR_NUM,
      raw_stg.CXKEY_FRMWRK_ID,
      raw_stg.CXKEY_SUBMITTING_ENTITY,
      raw_stg.ASRECDORCOMP_DATA_IND,
      raw_stg.CXKEY_APP_PLATFORM,
      raw_stg.CXKEY_APP_ID     from 
       ca_cal_test_reslt_raw_stg raw_stg
    left outer join
    CA_ASRECDORCOMP_DATA_TYP_STG stg
      on 
      stg.CXKEY_TEST_NM =raw_stg.CXKEY_TEST_NM AND
          stg.CXKEY_SRVC_ORD_NUM 			= raw_stg.CXKEY_SRVC_ORD_NUM 		AND
stg.LAST_MODIFIED_DT 			= raw_stg.LAST_MODIFIED_DT 		    AND
stg.CXKEY_CAL_PROCEDURE_ID 		= raw_stg.CXKEY_CAL_PROCEDURE_ID    AND
stg.CXKEY_MFGR_ID 				= raw_stg.CXKEY_MFGR_ID 			AND
stg.CXKEY_MFGR_MDL_NUM 			= raw_stg.CXKEY_MFGR_MDL_NUM 		AND
stg.CXKEY_SR_NUM 				= raw_stg.CXKEY_SR_NUM 			    AND
stg.CXKEY_FRMWRK_ID 			= raw_stg.CXKEY_FRMWRK_ID 		    AND
stg.CXKEY_SUBMITTING_ENTITY 	= raw_stg.CXKEY_SUBMITTING_ENTITY  
      where stg.ASRECDORCOMP_DATA_IND='AS_RECEIVED'
      ),
      joiner as
      (
        select lkptrans2.*,lkptrans1.AS_CPMPLT_CAL_TEST_DATA_TYP_ID,CA_CAL_RESLT_DATA_STG.CAL_RESLT_DATA_ID
        from lkptrans1 left outer join lkptrans2 on lkptrans1.CXKEY_TEST_NM=lkptrans2.CXKEY_TEST_NM
        join CA_CAL_RESLT_DATA_STG on CA_CAL_RESLT_DATA_STG.CXKEY_SRVC_ORD_NUM=lkptrans1.CXKEY_SRVC_ORD_NUM
     ),
      fil_trans as
      (
      select distinct
       (CA_CAL_TEST_RESLT_STG_SEQ.nextval) as CAL_TEST_RESLT_ID,
CXKEY_TEST_NM AS CXKEY_CAL_TEST_RESLT_NM,
AS_RECVD_CAL_TEST_DATA_TYP_ID,
AS_CPMPLT_CAL_TEST_DATA_TYP_ID,
CAL_RESLT_DATA_ID,
ASRECDORCOMP_DATA_IND,
null AS INSERT_DT,
null as UPD_DT,
CXKEY_SRVC_ORD_NUM,
LAST_MODIFIED_DT,
CXKEY_CAL_PROCEDURE_ID,
CXKEY_MFGR_ID,
CXKEY_MFGR_MDL_NUM,
CXKEY_SR_NUM,
CXKEY_FRMWRK_ID,
CXKEY_SUBMITTING_ENTITY,
CXKEY_APP_PLATFORM,
CXKEY_APP_ID
 from JOINER
      where 
      ASRECDORCOMP_DATA_IND is not null)
      select * from fil_trans