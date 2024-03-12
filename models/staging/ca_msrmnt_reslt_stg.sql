{{
    config(
        materialized='table',
    )
}}

with
    ca_msrmnt_reslt_raw_stg as (select * from ca_msrmnt_reslt_raw),
    exp_recd as (
        select
            *,
            'AS_RECEIVED' as as_received_resultstype,
            null as as_completed_resultstype
        from ca_msrmnt_reslt_raw_stg
        where "ResultsType" = 'AS_RECEIVED'
    ),
    exp_cmpl as (
        select
            *,
            null as as_received_resultstype,
            'AS_COMPLETED' as as_completed_resultstype
        from ca_msrmnt_reslt_raw_stg
        where "ResultsType" = 'AS_COMPLETED'
    ),
    data1 as (
        select *
        from exp_cmpl
        union all
        select *
        from exp_recd
    )
    ,
    exptrans1 as (
        select
            *,
            iff(
                as_received_resultstype = 'AS_COMPLETED',
                'E',
                iff(
                    as_completed_resultstype = 'AS_RECEIVED',
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
    )

    ,
    recd as (
        select
            null as msrmnt_samples_descr,
            "ImpliedLowerLimit" as implied_lwr_lmt,
            "ImpliedUpperLimit" as implied_upr_lmt,
            "Offset" as offset,
            "AdjustedCoverageFactor" as coverage_factor,
            "NearSpecificationLimit" as near_spcfctn_lmt_ind,
            "NearGuardbandLimit" as near_gurdbd_lmt_ind,
            "MeasResultID" as msrmnt_data_indntifier,
            "StandardJudgement" as std_jdgmnt,
            "GuardbandJudgement" as gurdbd_jdgmnt,
            "ProcessJudgement" as prcss_jdgmnt,
            "AsReceivedReportedJudgement" as rprtd_jdgmnt,
            null as insert_dt,
            null as upd_dt,
            "MeasurementData_TestCondition" as test_cond_name,
            "CXKEY_ServiceOrderNumber" as cxkey_srvc_ord_num,
            "CXKEY_LastModified" as last_modified_dt,
            "CXKEY_ProcedureID" as cxkey_cal_procedure_id,
            "CXKEY_ManufID" as cxkey_mfgr_id,
            "CXKEY_ManufModel" as cxkey_mfgr_mdl_num,
            "CXKEY_SerialNumber" as cxkey_sr_num,
            "CXKEY_AppFramework_FrameworkID" as cxkey_frmwrk_id,
            "CXKEY_SubmittingEntity" as cxkey_submitting_entity,
            "TestName" as cxkey_cal_test_reslt_nm,
            "ResultsType" as cxkey_cal_test_reslt_typ,
            "AppPlatform" as cxkey_app_platform,
            "AppID" as cxkey_app_id,
            "TestDataSectName" as cxkey_test_data_sctn_nm,
            "TestDataSectionID" as cxkey_test_data_sctn_unique_id,
            "MeasurementResult_TestCondition" as test_condition,
            "Condition2" as condition_2,
            -- null as short_data_id,
            "FLAG" as asrecdorcomp_data_ind
        from exptrans1
        where "ResultsType" = 'AS_RECEIVED'
    )
    ,

    compl as (
        select
            "AsCompletedReportedJudgement" as cal_tst_reportedjudgement,
            "AppPlatform" as as_cmp_appplatform,
            "AppID" as as_cmp_appid,
            "TestDataSectionID" as as_cmp_testdatasection_id,
            "Offset" as as_cmp_offset,
            "NearSpecificationLimit" as as_cmp_nrspecificatnlmt_ind,
            "ImpliedUpperLimit" as as_cmp_impliedupperlimit,
            "ImpliedLowerLimit" as as_cmp_impliedlowerlimit,
            "MeasResultID" as as_cmp_measresultid,
            "NearGuardbandLimit" as as_cmp_nearguardbandlimit,
            "AdjustedCoverageFactor" as as_cmp_adjustedcoveragefactor,
            "MeasurementData_TestCondition" as as_cmp_testcondition_nm,
            null as as_cmp_measurementsamples,
            "Judgement" as as_cmp_judgement,
            "GuardbandJudgement" as as_cmp_guardbandjudgement,
            "ProcessJudgement" as as_cmp_processjudgement,
            "StandardJudgement" as as_cmp_standardjudgement,
            "MeasurementData_TestCondition" as as_cmp_testcondition,
            "Condition2" as as_cmp_condition,
            "TestDataSectName" as as_cmp_name
        from exptrans1
        where "ResultsType" = 'AS_COMPLETED'
    ),
     union_trans as (
        select stg1.*, stg2.*
        from recd stg1
        --- inner join 
            left outer join compl stg2 on stg1.cxkey_cal_test_reslt_nm = stg2.as_cmp_name
   -- and stg1.CXKEY_TEST_DATA_SCTN_UNIQUE_ID=stg2.AS_CMP_TESTDATASECTION_ID
    ) select distinct raw_stg.*, stg.MSRMNT_ID as msrmnt_id,stg.short_data_id, ca_msrmnt_reslt_seq.nextval as MSRMNT_RESLT_ID
from union_trans raw_stg
left outer join
    ca_msrmnt_data_stg stg
    on stg.cxkey_srvc_ord_num = raw_stg.cxkey_srvc_ord_num
    and stg.last_modified_dt = raw_stg.last_modified_dt
    and stg.cxkey_cal_procedure_id = raw_stg.cxkey_cal_procedure_id
    and stg.cxkey_mfgr_id = raw_stg.cxkey_mfgr_id
    and stg.cxkey_mfgr_mdl_num = raw_stg.cxkey_mfgr_mdl_num
    and stg.cxkey_sr_num = raw_stg.cxkey_sr_num
    and stg.cxkey_frmwrk_id = raw_stg.cxkey_frmwrk_id
    and stg.cxkey_submitting_entity = raw_stg.cxkey_submitting_entity
