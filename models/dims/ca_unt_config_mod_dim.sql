{{
    config(
        materialized='incremental',
        unique_key='unt_config_mod_id'
    )
}}


-- with ca_unt_config_mod_dim_final as 
-- (
-- select 
--   unt_config_mod_id,
--   mfgr_mdl_num,
--   agt_mdl_num,
--   descr,
--   sr_num,
--   cust_unt_num,
--   firmware_ver_num,
--   mod_nm,
--   instld_opt,
--   addr_typ,
--   addr_val,
--   mfgr_id,
--   mfgr_nm,
--   insert_dt,
--   upd_dt
--  from CA_UNT_CONFIG_MOD_STG
        
-- )

-- select 
--   unt_config_mod_id,
--   mfgr_mdl_num,
--   agt_mdl_num,
--   descr,
--   sr_num,
--   cust_unt_num,
--   firmware_ver_num,
--   mod_nm,
--   instld_opt,
--   addr_typ,
--   addr_val,
--   mfgr_id,
--   mfgr_nm,
--   insert_dt,
--   upd_dt
-- from ca_unt_config_mod_dim_final
-- where 1 = 2


with ca_unt_config_mod_dim_final as 
(
select 
        STG.unt_config_mod_id,
        DIM.unt_config_mod_id as dim_unt_config_mod_id,
        STG.mfgr_mdl_num,
        STG.agt_mdl_num,
        STG.descr,
        STG.sr_num,
        STG.cust_unt_num,
        STG.firmware_ver_num,
        STG.mod_nm,
        STG.instld_opt,
        STG.addr_typ,
        STG.addr_val,
        STG.mfgr_id,
        STG.mfgr_nm,
        STG.insert_dt,
        STG.upd_dt
 from 
        {{ ref('ca_unt_config_mod_stg') }} STG
        LEFT OUTER JOIN 
        CA_UNT_CONFIG_MOD_DIM DIM                
        ON 
        STG.mfgr_mdl_num = DIM.mfgr_mdl_num
        AND STG.sr_num = DIM.sr_num
        AND STG.mfgr_id = DIM.mfgr_id
)

select distinct
  unt_config_mod_id,
  mfgr_mdl_num,
  agt_mdl_num,
  descr,
  sr_num,
  cust_unt_num,
  firmware_ver_num,
  mod_nm,
  instld_opt,
  addr_typ,
  addr_val,
  mfgr_id,
  mfgr_nm,
  insert_dt,
  upd_dt
from ca_unt_config_mod_dim_final
where dim_unt_config_mod_id IS NULL 