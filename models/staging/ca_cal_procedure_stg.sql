{{
    config(
        materialized='incremental',
        unique_key=['PROCEDURE_ID','PROCEDURE_VER_NUM','FRMWRK_VER_NUM']
    )
}}

with
    ca_cal_procedure_raw_stg as (
        select
            "ProcedureName" as PROCEDURE_NM,
            "ProcedureID" as PROCEDURE_ID,
            "ProcedureVersion" as PROCEDURE_VER_NUM,
            "AppFramework_FrameworkName" as FRMWRK_NM,
            "AppFramework_FrameworkID" as FRMWRK_ID,
            "AppFramework_FrameworkVersion" as FRMWRK_VER_NUM,
            "ProcedureReference" as PROCEDURE_REF_NUM
        from ca_cal_procedure_raw
    ),
    calculations as (
        select 
            stg.cal_procedure_id,
            raw_stg.PROCEDURE_NM,
            raw_stg.PROCEDURE_ID,
            raw_stg.PROCEDURE_VER_NUM,
            raw_stg.FRMWRK_NM,
            raw_stg.FRMWRK_ID,
            raw_stg.FRMWRK_VER_NUM,
            raw_stg.PROCEDURE_REF_NUM,
            case
                when
                    (
                        raw_stg.PROCEDURE_ID = stg.PROCEDURE_ID
                        and raw_stg.PROCEDURE_VER_NUM = stg.PROCEDURE_VER_NUM
                        and raw_stg.FRMWRK_VER_NUM = stg.FRMWRK_VER_NUM
                    )
                then 2
                else 1
            end as newlookuprow,
            decode(newlookuprow, 1, 'I', 2, 'U', 'R') as change_flag,
            case
                when
                    change_flag = 'I'   AND   raw_stg.PROCEDURE_ID is not null 
                then 'DD_INSERT'
                when
                    (
                        (stg.PROCEDURE_VER_NUM != raw_stg.PROCEDURE_VER_NUM  
                        OR stg.FRMWRK_VER_NUM != raw_stg.FRMWRK_VER_NUM) and change_flag != 'I'               
                    )
                then 'DD_UPDATE'
            end as rtr
        from ca_cal_procedure_raw_stg raw_stg
        left outer join
        ca_cal_procedure_stg stg
        on 
        raw_stg.PROCEDURE_ID = stg.PROCEDURE_ID
        and raw_stg.PROCEDURE_VER_NUM = stg.PROCEDURE_VER_NUM
        and raw_stg.FRMWRK_VER_NUM = stg.FRMWRK_VER_NUM
    ),
    upd_strtg_ins as (
        select 
        cal_procedure_id,    
        PROCEDURE_NM,
        PROCEDURE_ID,
        PROCEDURE_VER_NUM,
        FRMWRK_NM,
        FRMWRK_ID,
        FRMWRK_VER_NUM,
        PROCEDURE_REF_NUM,
        current_date() as insert_dt,
        to_date('12-31-9999', 'mm-dd-yyyy') as upd_dt
        from calculations
        where rtr = 'DD_INSERT'
    ),
    upd_strtg_ins_upd as (
        select 
        cal_procedure_id,    
        PROCEDURE_NM,
        PROCEDURE_ID,
        PROCEDURE_VER_NUM,
        FRMWRK_NM,
        FRMWRK_ID,
        FRMWRK_VER_NUM,
        PROCEDURE_REF_NUM,
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
        from upd_strtg_ins_upd
    ),
    final as (
        select 
        cal_procedure_id,    
        PROCEDURE_NM,
        PROCEDURE_ID,
        PROCEDURE_VER_NUM,
        FRMWRK_NM,
        FRMWRK_ID,
        FRMWRK_VER_NUM,
        PROCEDURE_REF_NUM,
        insert_dt,
        upd_dt
        from target
    )
    select distinct
    (case
     when cal_procedure_id is null then (CA_CAL_PROCEDURE_STG_SEQ.nextval) else cal_procedure_id  end)
            as cal_procedure_id,
            final.PROCEDURE_NM,
            final.PROCEDURE_ID,
            final.PROCEDURE_VER_NUM,
            final.FRMWRK_NM,
            final.FRMWRK_ID,
            final.FRMWRK_VER_NUM,
            final.PROCEDURE_REF_NUM,
            final.INSERT_DT,
            final.UPD_DT
            from final
     