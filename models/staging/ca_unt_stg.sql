{{
    config(
        materialized='incremental',
        unique_key=['unt_id']
    )
}}

with
    ca_unt_raw_stg as (
        select
            "ManufModel" as mfgr_mdl_num,
            "AGTModel" as agt_mdl_num,
            "Description" as unt_descr,
            "SerialNumber" as sr_num,
            "CustUnitNumber" as cust_unt_num,
            "FirmwareVersion" as firmware_ver_num,
            "SoftwareVersion" as sw_ver_num,
            "InstalledOptions" as instld_opt,
            "AddressType" as addr_typ,
            "AddressValue" as addr_val,
            "ManufID" as mfgr_id,
            "Manuf" as mfgr_nm
        from ca_unt_raw
    ),
    calculations as (
        select
            stg.unt_id,
            raw_stg.mfgr_mdl_num,
            raw_stg.agt_mdl_num,
            raw_stg.unt_descr,
            raw_stg.sr_num,
            raw_stg.cust_unt_num,
            raw_stg.firmware_ver_num,
            raw_stg.sw_ver_num,
            raw_stg.instld_opt,
            raw_stg.addr_typ,
            raw_stg.addr_val,
            raw_stg.mfgr_id,
            raw_stg.mfgr_nm,
            case
                when
                    (
                        raw_stg.mfgr_mdl_num = stg.mfgr_mdl_num
                        and raw_stg.mfgr_id = stg.mfgr_id
                        and raw_stg.sr_num = stg.sr_num
                    )
                then 2
                else 1
            end as newlookuprow,
            decode(newlookuprow, 1, 'I', 2, 'U', 'R') as change_flag,
            case
                when
                    change_flag = 'I'
                    and raw_stg.mfgr_mdl_num is not null
                    and raw_stg.mfgr_id is not null
                    and raw_stg.sr_num is not null
                then 'DD_INSERT'
                when
                    (
                        stg.firmware_ver_num != raw_stg.firmware_ver_num
                        or stg.sw_ver_num != raw_stg.sw_ver_num
                    )
                    and change_flag != 'I'
                then 'DD_UPDATE'
            end as rtr
        from ca_unt_raw_stg raw_stg
        left outer join
            ca_unt_stg stg
            on raw_stg.mfgr_mdl_num = stg.mfgr_mdl_num
            and raw_stg.mfgr_id = stg.mfgr_id
            and raw_stg.sr_num = stg.sr_num
    ),
    upd_strtg_ins as (
        select
            ca_unt_stg_seq.nextval as unt_id,
            mfgr_mdl_num,
            agt_mdl_num,
            unt_descr,
            sr_num,
            cust_unt_num,
            firmware_ver_num,
            sw_ver_num,
            instld_opt,
            addr_typ,
            addr_val,
            mfgr_id,
            mfgr_nm,
            current_date() as insert_dt,
            to_date('12-31-9999', 'mm-dd-yyyy') as upd_dt
        from calculations
        where rtr = 'DD_INSERT'
    ),
    upd_strtg_upd as (
        select
            unt_id,
            mfgr_mdl_num,
            agt_mdl_num,
            unt_descr,
            sr_num,
            cust_unt_num,
            firmware_ver_num,
            sw_ver_num,
            instld_opt,
            addr_typ,
            addr_val,
            mfgr_id,
            mfgr_nm,
            current_date() as insert_dt,
            to_date('12-31-9999', 'mm-dd-yyyy') as upd_dt
        from calculations
        where rtr = 'DD_UPDATE'
    ),
    target as (
        select *
        from upd_strtg_ins
        union all
        select *
        from upd_strtg_upd
    ),

    final as (
        select
            unt_id,
            mfgr_mdl_num,
            agt_mdl_num,
            unt_descr,
            sr_num,
            cust_unt_num,
            firmware_ver_num,
            sw_ver_num,
            instld_opt,
            addr_typ,
            addr_val,
            mfgr_id,
            mfgr_nm,
            insert_dt,
            upd_dt
        from target
    )
select distinct *
from final