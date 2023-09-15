{{
    config(
        materialized='incremental',
        unique_key='prvdr_enty_cd'
    )
}}

with ca_srvc_prvdr_raw_stg as (

    select
        "ServiceProvider" as srvc_prvdr_nm,
        "ServiceAddress1" as addr_line_1,
        "ServiceAddress2" as addr_line_2,
        "ServiceAddress3" as  addr_line_3,
        "ProviderEntity" as  prvdr_enty_cd,
        "SubmittingEntity" as submitting_entity
//        case when "ServiceProvider" is null then "SubmittingEntity" else "ServiceProvider" end as Var_ServiceProvider
    from ca_srvc_prvdr_raw
  
),
calculation as (
    select
//        (case when srvc_prvdr_id is null then (select CA_SRVC_PRVDR_STG_SEQ.nextval)  else srvc_prvdr_id end) as srvc_prvdr_id,
            stg.srvc_prvdr_id as srvc_prvdr_id,
  
            case
                when
                    (
                       raw_stg.prvdr_enty_cd = stg.prvdr_enty_cd
                    )
                then 2
                else 1
            end as newlookuprow,
            decode(newlookuprow, 1, 'I', 2, 'U', 'R') as change_flag,
            case
                when
                    change_flag = 'I'   AND   raw_stg.prvdr_enty_cd is not null 
                then 'DD_INSERT'
                when
                    (
                        (raw_stg.prvdr_enty_cd != stg.prvdr_enty_cd) and change_flag != 'I'               
                    )
                then 'DD_UPDATE'
            end as rtr,
        raw_stg.srvc_prvdr_nm,
        raw_stg.addr_line_1,
        raw_stg.addr_line_2,
        raw_stg.addr_line_3,
        raw_stg.prvdr_enty_cd,
        raw_stg.submitting_entity,
        CURRENT_DATE() as insert_dt,
        TO_DATE('12-31-9999', 'mm-dd-yyyy' ) as upd_dt ,
        CURRENT_TIMESTAMP()  as test
        from ca_srvc_prvdr_raw_stg raw_stg 
        left outer join
        ca_srvc_prvdr_stg stg 
        on raw_stg.prvdr_enty_cd = stg.prvdr_enty_cd

),
upd_strtg_ins as (
  select
     srvc_prvdr_id,
      srvc_prvdr_nm,
        addr_line_1,
        addr_line_2,
        addr_line_3,
        prvdr_enty_cd,
        submitting_entity,
        insert_dt,
        upd_dt 
      from calculation
      where rtr = 'DD_INSERT'
      
      )
      ,
      upd_strtg_upd as (
  select
        srvc_prvdr_id,
      srvc_prvdr_nm,
        addr_line_1,
        addr_line_2,
        addr_line_3,
        prvdr_enty_cd,
        submitting_entity,
        insert_dt,
        upd_dt 
      from calculation
      where rtr = 'DD_UPDATE'
      
      ) ,
      target as (
        select *
        from upd_strtg_ins
        union all
        select *
        from upd_strtg_upd
    ),
    final as (
      select 
      srvc_prvdr_id,
      srvc_prvdr_nm,
        addr_line_1,
        addr_line_2,
        addr_line_3,
        prvdr_enty_cd,
        submitting_entity,
        insert_dt,
        upd_dt 
      from target
    )    
    select distinct
       (case when srvc_prvdr_id is null then (ca_srvc_prvdr_stg_seq.nextval)  else srvc_prvdr_id end) as srvc_prvdr_id,
        srvc_prvdr_nm,
        addr_line_1,
        addr_line_2,
        addr_line_3,
        prvdr_enty_cd,
        submitting_entity,
        insert_dt,
        upd_dt 
 from final 