{{
    config(
        materialized='incremental',
        unique_key=['UNT_ID']
    )
}}



with ca_unt_dim_final as 
(
select 
    DIM.MFGR_MDL_NUM as DIM_MFGR_MDL_NUM,
    STG.MFGR_MDL_NUM,
	STG.AGT_MDL_NUM,
	STG.UNT_DESCR,
	STG.SR_NUM,
	STG.CUST_UNT_NUM,
	STG.FIRMWARE_VER_NUM,
	STG.SW_VER_NUM,
	STG.UNT_ID,
	STG.INSTLD_OPT,
	STG.ADDR_TYP,
	STG.ADDR_VAL,
	STG.MFGR_ID,
	STG.MFGR_NM,
	STG.INSERT_DT,
	STG.UPD_DT,
    case
        when STG.MFGR_ID = 'HWP' then 'AGT'
        else STG.MFGR_ID
    end as MFGR_ID_FOR_AGGREGATES
 from 
        {{ ref('ca_unt_stg') }} STG
        LEFT OUTER JOIN 
        CA_UNT_DIM DIM                
        ON 
        STG.MFGR_MDL_NUM = DIM.MFGR_MDL_NUM
        AND STG.SR_NUM = DIM.SR_NUM
        AND STG.MFGR_ID = DIM.MFGR_ID
)

select distinct
    MFGR_MDL_NUM,
	AGT_MDL_NUM,
	UNT_DESCR as DESCR,
	SR_NUM,
	CUST_UNT_NUM,
	FIRMWARE_VER_NUM as FIRMWARE_VER,
	SW_VER_NUM as SW_VER,
	UNT_ID,
	INSTLD_OPT,
	ADDR_TYP,
	ADDR_VAL,
	MFGR_ID,
	MFGR_NM,
	INSERT_DT as INSERT_DATE,
	UPD_DT as UPDATE_DATE,
	MFGR_ID_FOR_AGGREGATES
from ca_unt_dim_final
--where DIM_MFGR_MDL_NUM IS NULL 