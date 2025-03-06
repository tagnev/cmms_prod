{{ config(materialized="table") }}

with
    ca_test_data_sctn_hdr_raw_stg as (select * from ca_test_data_sctn_hdr_raw),
    exp_recd as (
        select
            *,
            'AS_RECEIVED' as as_received_resultstype,
            null as as_completed_resultstype
        from ca_test_data_sctn_hdr_raw_stg
        where "AsRecCompType" = 'AsReceived'
    ),
    exp_cmpl as (
        select
            *,
            null as as_received_resultstype,
            'AS_COMPLETED' as as_completed_resultstype
        from ca_test_data_sctn_hdr_raw_stg
        where "AsRecCompType" = 'AsCompleted'
    ),
    data1 as (
        select *
        from exp_cmpl
        union all
        select *
        from exp_recd
    ),
    exptrans1 as (
        select
            *,
            iff(
                as_received_resultstype = 'AS_RECEIVED',
                'E',
                iff(
                    as_completed_resultstype = 'AS_COMPLETED',
                    'E',
                    iff(
                        as_received_resultstype = 'BOTH',
                        'B',
                        iff(
                            as_completed_resultstype = 'AS_COMPLETED'
                            and as_received_resultstype is null,
                            'C',
                            iff(
                                as_received_resultstype = 'AS_RECEIVED'
                                and as_completed_resultstype is null,
                                'R',
                                iff(
                                    as_received_resultstype = 'AS_RECEIVED'
                                    and as_completed_resultstype = 'AS_COMPLETED',
                                    'A',
                                    'X'
                                )
                            )
                        )
                    )
                )
            ) as flag,
            iff(flag != 'A', true, false) as other_data_indicators
        from data1
    ),
    lkptrans as (
        select
            stg.test_data_sctn_id,
            stg.cxkey_cal_test_reslt_nm,
            stg.cxkey_cal_test_reslt_typ,
            stg.test_data_id,
            raw_stg.*
        from exptrans1 raw_stg
        left outer join
            ca_test_data_sctn_stg stg
            on stg.cxkey_cal_test_reslt_nm = raw_stg."Name"
            and stg.test_data_id = raw_stg."TestDataSectionID"
            and stg.cxkey_srvc_ord_num = raw_stg."CXKEY_ServiceOrderNumber"
            and stg.last_modified_dt = raw_stg."CXKEY_LastModified"
            and stg.cxkey_cal_procedure_id = raw_stg."CXKEY_ProcedureID"
            and stg.cxkey_mfgr_id = raw_stg."CXKEY_ManufID"
            and stg.cxkey_mfgr_mdl_num = raw_stg."CXKEY_ManufModel"
            and stg.cxkey_sr_num = raw_stg."CXKEY_SerialNumber"
            and stg.cxkey_frmwrk_id = raw_stg."CXKEY_AppFramework_FrameworkID"
            and stg.cxkey_submitting_entity = raw_stg."CXKEY_SubmittingEntity"
    ), filter as
    (
        select *
        from lkptrans
        where test_data_sctn_id is not null or test_data_sctn_id = to_decimal('')
    ),
    final as (
        select
            "HeaderComment" as cxkey_hdr_commnt,
            test_data_sctn_id,
            null as insert_dt,
            null as upd_dt,
            "CXKEY_ServiceOrderNumber" as cxkey_srvc_ord_num,
            "CXKEY_LastModified" as last_modified_dt,
            "CXKEY_ProcedureID" as cxkey_cal_procedure_id,
            "CXKEY_ManufID" as cxkey_mfgr_id,
            "CXKEY_ManufModel" as cxkey_mfgr_mdl_num,
            "CXKEY_SerialNumber" as cxkey_sr_num,
            "CXKEY_AppFramework_FrameworkID" as cxkey_frmwrk_id,
            "CXKEY_SubmittingEntity" cxkey_submitting_entity,
            "AppPlatform" as cxkey_app_platform,
            "AppID" as cxkey_app_id,
            cxkey_cal_test_reslt_nm,
            cxkey_cal_test_reslt_typ,
            test_data_id as cxkey_cal_test_data_id,
            "Name" as cxkey_test_data_sctn_nm
        from filter
    )
select distinct *
from final
