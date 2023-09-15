{{
    config(
        materialized='incremental',
        unique_key='EQPMNT_ID'
    )
}}


with CA_EQPMNT_DIM_FINAL as 
(
select distinct
    DIM.MFGR_ID as DIM_MFGR_ID,
    STG.MFGR_MDL_NUM,
	STG.DESCR,
	STG.SR_NUM,
	STG.TRC_NUM,
	STG.ASSET_ID,
	STG.CAL_DUE_DT,
	STG.EQPMNT_ID,
	STG.INSTLD_OPT,
	STG.MFGR_NM,
	STG.MFGR_ID,
	STG.INSERT_DT,
	STG.UPD_DT
    from 
      {{ ref('ca_eqpmnt_stg') }} STG
      LEFT OUTER JOIN 
      CA_EQPMNT_DIM DIM                
      ON 
      STG.MFGR_MDL_NUM = DIM.MFGR_MDL_NUM
      AND STG.SR_NUM = DIM.SR_NUM
      AND STG.MFGR_ID = DIM.MFGR_ID
      AND STG.ASSET_ID = DIM.ASSET_ID
      AND STG.CAL_DUE_DT = STG.CAL_DUE_DT
)

select distinct
    DESCR,
    SR_NUM,
    TRC_NUM,
    ASSET_ID,
    CAL_DUE_DT,
    EQPMNT_ID,
    INSTLD_OPT,
    MFGR_ID,
    MFGR_NM,
    MFGR_MDL_NUM,
    INSERT_DT,
    UPD_DT
from CA_EQPMNT_DIM_FINAL
where DIM_MFGR_ID IS NULL 
