{{ config(materialized="incremental", unique_key=["unt_id", "unt_config_mod_id"]) }}

with
    ca_unt_to_unt_config_mod_raw_stg as (
        select
            "ManufModel" as mfgr_mdl_num, "ManufID" as mfgr_id, "SerialNumber" as sr_num
        from ca_unt_raw
    ),
    lkptrans as (
        select stg.unt_config_mod_id, raw_stg.*
        from ca_unt_to_unt_config_mod_raw_stg raw_stg
        left outer join
            ca_unt_config_mod_stg stg
            on raw_stg.mfgr_mdl_num = stg.mfgr_mdl_num
            and raw_stg.mfgr_id = stg.mfgr_id
            and raw_stg.sr_num = stg.sr_num
    ),
    lkptrans1 as (
        select stg.unt_id, raw_stg.*
        from lkptrans raw_stg
        left outer join
            ca_unt_stg stg
            on raw_stg.mfgr_mdl_num = stg.mfgr_mdl_num
            and raw_stg.mfgr_id = stg.mfgr_id
            and raw_stg.sr_num = stg.sr_num
    ),
    lkptrans2 as (
        select
            raw_stg.unt_id,
            raw_stg.unt_config_mod_id,
            stg.unt_id as in_unt_id,
            stg.unt_config_mod_id as in_unt_config_mod_id
        from lkptrans1 raw_stg
        left outer join
            ca_unt_to_unt_config_mod_stg stg
            on raw_stg.unt_id = stg.unt_id
            and raw_stg.unt_config_mod_id = stg.unt_config_mod_id
    ),
    expr_checkupdate as (
        select
            unt_id,
            in_unt_id,
            unt_config_mod_id,
            in_unt_config_mod_id,
            iff(
                (unt_id) is not null and (unt_config_mod_id) is not null, 'R', 'I'
            ) change_flag
        from lkptrans2
    ),
    final as (
        select unt_config_mod_id, unt_id, null as insert_dt, null as upd_dt
        from expr_checkupdate
        where
            change_flag = 'I'
            and (in_unt_id) is not null
            and (in_unt_config_mod_id) is not null
    )
select distinct *
from final
