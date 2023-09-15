{{
    config(
        materialized='incremental',
        unique_key='prvdr_enty_cd'
    )
}}

-- with ca_srvc_prvdr_dim_final as 
-- (
-- select 
-- srvc_prvdr_id,
--         srvc_prvdr_nm,
--         addr_line_1,
--         addr_line_2,
--         addr_line_3,
--         prvdr_enty_cd,
--         submitting_entity,
--         insert_dt,
--         upd_dt 
--  from 
        
--         CA_SRVC_PRVDR_STG STG
        
-- )

-- select
--         srvc_prvdr_id,
--         srvc_prvdr_nm,
--         addr_line_1,
--         addr_line_2,
--         addr_line_3,
--         prvdr_enty_cd,
--         submitting_entity,
--         insert_dt,
--         upd_dt 
-- from
-- ca_srvc_prvdr_dim_final
-- where 1=2

with ca_srvc_prvdr_dim_final as 
(
select 
        DIM.srvc_prvdr_id as dim_srvc_prvdr_id,
        STG.srvc_prvdr_id,
        STG.srvc_prvdr_nm,
        STG.addr_line_1,
        STG.addr_line_2,
        STG.addr_line_3,
        STG.prvdr_enty_cd,
        STG.submitting_entity,
        STG.insert_dt,
        STG.upd_dt 
 from 
        {{ ref('ca_srvc_prvdr_stg') }} STG
        LEFT OUTER JOIN 
        CA_SRVC_PRVDR_DIM DIM                
        ON 
        STG.prvdr_enty_cd = DIM.prvdr_enty_cd
)

select distinct
        srvc_prvdr_id,
        srvc_prvdr_nm,
        addr_line_1,
        addr_line_2,
        addr_line_3,
        insert_dt as INSERT_DATE,
        upd_dt as UPDATE_DATE,
        prvdr_enty_cd,
        submitting_entity
from
ca_srvc_prvdr_dim_final
where dim_srvc_prvdr_id is null