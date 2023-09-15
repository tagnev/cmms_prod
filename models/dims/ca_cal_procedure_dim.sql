{{
    config(
        materialized='incremental',
        unique_key='CAL_PROCEDURE_ID'
    )
}}

with calculations as (
        select 
            dim.CAL_PROCEDURE_ID as DIM_CAL_PROCEDURE_ID,
            stg.PROCEDURE_NM,
            stg.CAL_PROCEDURE_ID,
            stg.PROCEDURE_VER_NUM,
            stg.PROCEDURE_REF_NUM,
            stg.FRMWRK_NM,
            stg.FRMWRK_VER_NUM,
            stg.FRMWRK_ID,
            stg.INSERT_DT,
            stg.UPD_DT,
            stg.PROCEDURE_ID,
            case
                when
                    (
                        dim.CAL_PROCEDURE_ID IS null
                    )
                then 'I'
                else 'U'
            end as flag,
            case
                when flag = 'I'then 'DD_INSERT'
                when flag = 'U'then 'DD_UPDATE'
            end as rtr
            from {{ ref('ca_cal_procedure_stg') }} stg
            left outer join
            CA_CAL_PROCEDURE_DIM dim
            on 
            stg.CAL_PROCEDURE_ID = dim.CAL_PROCEDURE_ID
     
       ),
        upd_strtg_ins as (
        select 
        PROCEDURE_NM,
        CAL_PROCEDURE_ID,
        PROCEDURE_VER_NUM,
        PROCEDURE_REF_NUM,
        FRMWRK_NM,
        FRMWRK_VER_NUM,
        FRMWRK_ID,
        INSERT_DT,
        UPD_DT,
        PROCEDURE_ID
        from calculations
        where rtr = 'DD_INSERT'
    ),
    upd_strtg_ins_upd as (
        select 
        PROCEDURE_NM,
        CAL_PROCEDURE_ID,
        PROCEDURE_VER_NUM,
        PROCEDURE_REF_NUM,
        FRMWRK_NM,
        FRMWRK_VER_NUM,
        FRMWRK_ID,
        INSERT_DT,
        UPD_DT,
        PROCEDURE_ID
        from calculations
        where rtr = 'DD_UPDATE'
    ),
    target as (
        select *
        from upd_strtg_ins
        union all
        select *
        from upd_strtg_ins_upd
    )
    select distinct
    PROCEDURE_NM,
    CAL_PROCEDURE_ID,
    PROCEDURE_VER_NUM,
    PROCEDURE_REF_NUM,
    FRMWRK_NM,
    FRMWRK_VER_NUM,
    FRMWRK_ID,
    PROCEDURE_ID,
    INSERT_DT,
    UPD_DT
    from target
