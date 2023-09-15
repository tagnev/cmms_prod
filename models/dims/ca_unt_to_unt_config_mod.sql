{{
    config(
        materialized='incremental',
        unique_key='UNT_CONFIG_MOD_ID'
    )
}}

with CA_UNT_TO_UNT_CONFIG_MOD_FINAL as 
(
select 
    FIN.UNT_CONFIG_MOD_ID as FIN_UNT_CONFIG_MOD_ID,
    STG.UNT_CONFIG_MOD_ID,
	STG.UNT_ID
 from 
        {{ ref('ca_unt_to_unt_config_mod_stg') }} STG
        LEFT OUTER JOIN 
        CA_UNT_TO_UNT_CONFIG_MOD FIN                
        ON 
        STG.UNT_CONFIG_MOD_ID = FIN.UNT_CONFIG_MOD_ID
        AND STG.UNT_ID = FIN.UNT_ID
)

select distinct
    UNT_CONFIG_MOD_ID,
	UNT_ID
from CA_UNT_TO_UNT_CONFIG_MOD_FINAL
where FIN_UNT_CONFIG_MOD_ID IS NULL 