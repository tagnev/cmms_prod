{{
    config(
        materialized='incremental',
        unique_key=['mfgr_mdl_num','mfgr_id','sr_num','trc_num','instld_opt']
    )
}}

with
    ca_eqpmnt_raw_stg as (
        select distinct   "ManufID" as mfgr_id,
            "Manuf" as mfgr_nm,
            "ManufModel" as mfgr_mdl_num,
            "Description" as descr,
            "SerialNumber" as sr_num,
            "TraceNumber" as trc_num,
            "AssetID" as asset_id,
            "CalDueDate" as cal_due_dt,
            "InstalledOptions" as instld_opt,
            std_or_acc_identifier
        from ca_eqpmnt_raw
    ),
    exp_standard as (
        select
            mfgr_id,
            mfgr_nm,
            mfgr_mdl_num,
            instld_opt,
            descr,
            sr_num,
            IFF(trc_num is null,'NO STD TRACE NUMBER',trc_num) as trc_num,
            asset_id,
            cal_due_dt,
            std_or_acc_identifier
        from ca_eqpmnt_raw_stg
        where std_or_acc_identifier = 'S'
    ),
    exp_accessory as (
        select
            mfgr_id,
            mfgr_nm,
            mfgr_mdl_num,
            descr,
            instld_opt,
            sr_num,
            'NO ACC TRACE NUMBER' as trc_num,
            'EMPTY' as asset_id,
            to_date('12-31-9999', 'MM-DD-YYYY') as cal_due_dt,
            std_or_acc_identifier
        from ca_eqpmnt_raw_stg
        where std_or_acc_identifier = 'A'
    ),
    union_trans as (
        select *
        from exp_standard
        union all
        select *
        from exp_accessory
    )
    ,
    calculations as (
        select 
            stg.eqpmnt_id,
            raw_stg.asset_id,
            raw_stg.cal_due_dt,
            raw_stg.instld_opt,
            raw_stg.mfgr_id,
            raw_stg.mfgr_mdl_num,
            raw_stg.sr_num,
            raw_stg.std_or_acc_identifier,
            raw_stg.descr,
            raw_stg.trc_num,
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
                    and raw_stg.std_or_acc_identifier='S'
                    and raw_stg.mfgr_mdl_num is not null
                    and raw_stg.mfgr_id is not null
                    and raw_stg.sr_num is not null
                then 'INSERT_STANDARD'
                when
                    (
                        change_flag = 'I'
                    and raw_stg.std_or_acc_identifier='A'
                    and raw_stg.mfgr_mdl_num is not null
                    and raw_stg.mfgr_id is not null
                    and raw_stg.sr_num is not null
                    )
                then 'INSERT_ACC'
            end as rtr
        from union_trans raw_stg
        left outer join
        ca_eqpmnt_stg stg
        on 
        raw_stg.ASSET_ID = raw_stg.ASSET_ID
        AND raw_stg.CAL_DUE_DT = stg.CAL_DUE_DT
        AND raw_stg.MFGR_ID = stg.MFGR_ID 
        AND raw_stg.MFGR_MDL_NUM = stg.MFGR_MDL_NUM 
        AND raw_stg.SR_NUM = stg.SR_NUM
    )
    ,
    upd_strtg_ins_std as (
        select 
        eqpmnt_id,    
        asset_id,
        mfgr_nm,
        instld_opt,
        cal_due_dt,
        descr,
        trc_num,
        mfgr_id,
        mfgr_mdl_num,
        sr_num,
        std_or_acc_identifier,
        current_date() as insert_dt,
        to_date('12-31-9999', 'mm-dd-yyyy') as upd_dt
        from calculations
        where rtr = 'INSERT_STANDARD'
    ),
    upd_strtg_ins_acc as (
        select 
        eqpmnt_id,    
        asset_id,
        mfgr_nm,
        instld_opt,
        cal_due_dt,
        descr,
        trc_num,
        mfgr_id,
        mfgr_mdl_num,
        sr_num,
        std_or_acc_identifier,
        current_date() as insert_dt,
        to_date('12-31-9999', 'mm-dd-yyyy') as upd_dt
        from calculations
        where rtr = 'INSERT_ACC'
    ),
    target as (
        select *
        from upd_strtg_ins_std
        union all
        select *
        from upd_strtg_ins_acc
    
    )
    ,
    final as (
        select distinct
        eqpmnt_id,
        asset_id,
        mfgr_nm,
        cal_due_dt,
        instld_opt,
        descr,
        trc_num,
        mfgr_id,
        mfgr_mdl_num,
        sr_num,
        std_or_acc_identifier,
        insert_dt,
        upd_dt
        from target
    )
    select distinct final.mfgr_mdl_num,
    final.descr,
    final.sr_num,
    final.trc_num,
    final.asset_id,
    final.cal_due_dt,
    (case
     when eqpmnt_id is null then (CA_EQPMNT_STG_SEQ.nextval) else eqpmnt_id  end)
            as eqpmnt_id,
            final.instld_opt,
            //to_varchar(final.cal_due_dt) as cal_due_dt,
            final.mfgr_nm,       
            final.mfgr_id,
            final.insert_dt,
            final.upd_dt,
            final.std_or_acc_identifier
            from final

            {% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  where insert_dt >= (select max(insert_dt) from {{ this }})

{% endif %}