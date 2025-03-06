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
            null as short_data_id,
            "FLAG" as asrecdorcomp_data_ind
        from exptrans1
        where "ResultsType" = 'AS_RECEIVED'
    ),

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
        inner join compl stg2 on stg1.cxkey_cal_test_reslt_nm = stg2.as_cmp_name
    -- and stg1.CXKEY_TEST_DATA_SCTN_UNIQUE_ID=stg2.AS_CMP_TESTDATASECTION_ID
    ),
    ca_uncrtnty_raw_stg as (

        select distinct "AsReceivedReportedJudgement" ,
            "AsCompletedReportedJudgement" ,
            "TestName" ,
            "AppPlatform",
            "AppID" ,
            "ResultsType",
            "TestDataSectName" ,
            "TestDataSectionID",
            "Uncertainty",
            "Type",
            "Units" ,
            "IsAppliedUncertainty" ,
            "Name",
            "MetrologyDatabase",
            "TestCondition" ,
            "Condition2" ,
            "IsAppliedProcessUncertainty",
            "CXKEY_ProcedureID",
            "CXKEY_ManufID" ,
            "CXKEY_ManufModel",
            "CXKEY_SerialNumber",
            "CXKEY_AppFramework_FrameworkID" ,
            "CXKEY_SubmittingEntity" ,
            "CXKEY_LastModified",
            "CXKEY_ServiceOrderNumber"
            from CA_UNCRTNTY_TYP_RAW
    ),
    join_msr_uncr as (
        select  distinct 
--         raw."AsReceivedReportedJudgement" ,
-- raw."AsCompletedReportedJudgement" ,
raw."TestName" as CXKEY_CAL_TEST_RESLT_NM,
raw."AppPlatform" as CXKEY_APP_PLATFORM,
raw."AppID" as CXKEY_APP_ID ,
raw."ResultsType" as CXKEY_CAL_TEST_RESLT_TYP,
raw."TestDataSectName"  as CXKEY_TEST_DATA_SCTN_NM,
raw."TestDataSectionID" as CXKEY_TEST_DATA_SCTN_UNIQUE_ID,
raw."Uncertainty" as UNCRTNTY,
raw."Type" as TYP,
raw."Units"  as UNT,
raw."IsAppliedUncertainty" as IS_APPLD_UNCRTNTY,
raw."Name" as NM,
raw."MetrologyDatabase" as METROLOGY_DATABASE_NM,
raw."TestCondition" as TEST_CONDITION,
raw."Condition2"  as CONDITION_2,
raw."IsAppliedProcessUncertainty" as IS_APPLD_PRCSS_UNCRTNTY,
raw."CXKEY_ProcedureID" as CXKEY_CAL_PROCEDURE_ID,
raw."CXKEY_ManufID" as CXKEY_MFGR_ID ,
raw."CXKEY_ManufModel" as CXKEY_MFGR_MDL_NUM,
raw."CXKEY_SerialNumber" as CXKEY_SR_NUM,
raw."CXKEY_AppFramework_FrameworkID" as CXKEY_FRMWRK_ID ,
raw."CXKEY_SubmittingEntity" as CXKEY_SUBMITTING_ENTITY ,
raw."CXKEY_LastModified" as LAST_MODIFIED_DT,
raw."CXKEY_ServiceOrderNumber" as CXKEY_SRVC_ORD_NUM,
raw_msr."MeasResultID" as CXKEY_MSRMNT_ID
 from 
        CA_UNCRTNTY_TYP_RAW raw 
        join 
        ca_msrmnt_reslt_raw raw_msr  
        on raw."TestDataSectionID" = raw_msr."TestDataSectionID"
        and raw."TestDataSectName" = raw_msr."TestDataSectName"
        and raw."CXKEY_ServiceOrderNumber" = raw_msr."CXKEY_ServiceOrderNumber"
        and raw."ResultsType" = raw_msr."ResultsType"
        -- where  raw."TestDataSectionID"
        -- in (select distinct "TestDataSectionID" from CA_UNCRTNTY_TYP_RAW)
        -- ca_msrmnt_reslt_raw_stg msr 
        -- on raw."TestDataSectionID" = msr."TestDataSectionID"
        -- and  raw."CXKEY_ServiceOrderNumber" = msr."CXKEY_ServiceOrderNumber"
        -- and raw."TestDataSectionID"='V08tMDA0MTcxNTBTVEU1MDExNDU2OUZSRVFVRU5DWUFDQ1VSQUNZ-0'
    ),
       final as (
        select 
        ca_uncrtnty_tbl.*,
        stg.MSRMNT_RESLT_ID,       
        stg.SHORT_DATA_ID
         from 
        join_msr_uncr ca_uncrtnty_tbl
        left outer join
        CA_MSRMNT_RESLT_STG stg 
        on ca_uncrtnty_tbl.CXKEY_MSRMNT_ID = stg.MSRMNT_ID
        and ca_uncrtnty_tbl.CXKEY_SRVC_ORD_NUM = stg.CXKEY_SRVC_ORD_NUM
        and to_date(ca_uncrtnty_tbl.LAST_MODIFIED_DT) = to_date(stg.LAST_MODIFIED_DT)
        and ca_uncrtnty_tbl.CXKEY_CAL_PROCEDURE_ID = stg.CXKEY_CAL_PROCEDURE_ID
        and ca_uncrtnty_tbl.CXKEY_MFGR_ID = stg.CXKEY_MFGR_ID
        and ca_uncrtnty_tbl.CXKEY_MFGR_MDL_NUM = stg.CXKEY_MFGR_MDL_NUM
        and ca_uncrtnty_tbl.CXKEY_SR_NUM = stg.CXKEY_SR_NUM
        and ca_uncrtnty_tbl.CXKEY_FRMWRK_ID = stg.CXKEY_FRMWRK_ID
        and ca_uncrtnty_tbl.CXKEY_SUBMITTING_ENTITY = stg.CXKEY_SUBMITTING_ENTITY
        and ca_uncrtnty_tbl.CXKEY_CAL_TEST_RESLT_NM = stg.CXKEY_CAL_TEST_RESLT_NM
        -- and ca_uncrtnty_tbl.CXKEY_APP_PLATFORM = stg.CXKEY_APP_PLATFORM
        -- and ca_uncrtnty_tbl.CXKEY_APP_ID = stg.CXKEY_APP_ID
        and ca_uncrtnty_tbl.CXKEY_CAL_TEST_RESLT_TYP = stg.CXKEY_CAL_TEST_RESLT_TYP
        and ca_uncrtnty_tbl.CXKEY_TEST_DATA_SCTN_NM = stg.CXKEY_TEST_DATA_SCTN_NM
        and ca_uncrtnty_tbl.CXKEY_TEST_DATA_SCTN_UNIQUE_ID = stg.CXKEY_TEST_DATA_SCTN_UNIQUE_ID
        where stg.MSRMNT_RESLT_ID is not null
        

    )
select distinct * from final

-- 3698162