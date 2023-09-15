{{ config(materialized="table") }}

with
    ca_test_data_sctn_raw_stg as (select distinct * from ca_test_data_sctn_raw),
    exp_recd as (
        select distinct
            *,
            'AS_RECEIVED' as as_received_resultstype,
            null as as_completed_resultstype
        from ca_test_data_sctn_raw
        where as_rec_or_comp = 'AsReceived'
    ),
    exp_cmpl as (
        select distinct
            *,
            null as as_received_resultstype,
            'AS_COMPLETED' as as_completed_resultstype
        from ca_test_data_sctn_raw
        where as_rec_or_comp = 'AsCompleted'
    ),
    data1 as (
        select distinct *
        from exp_cmpl
        union all
        select distinct *
        from exp_recd
    ),
    exptrans1 as (
        select distinct
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
    ),
    compl as (
        select distinct
            "TestDataSectionID" as as_cmp_tst_data_sec_id,
            "Name" as as_cmp_tst_data_sctn_nm,
            "Title" as as_cmp_sctn_title,
            "AppliedUncertainty" as as_cmp_appld_uncrtnty_typ,
            "ReportLimitsAsTolerance" as as_cmp_rprtd_lmts_as_tlrnc_ind,
            "ReportMeasuredAsDifference" as as_cmp_rprtd_msrd_as_dff_ind,
            "NearGuardbandLimit" as as_cmp_nr_gurdbd_lmt_ind,
            "NearSpecificationLimit" as as_cmp_nr_spcfctn_lmt_ind,
            TRY_TO_NUMBER("IsCharacterizationData") as as_cmp_is_chrtn_data,
            "AsCompletedReportedJudgement" as as_cmp_rprtd_jdgmnt,
            "StandardJudgement" as as_cmp_stnd_jdgmnt,
            "GuardbandJudgement" as as_cmp_gurdbd_jdg,
            "ProcessJudgement" as as_cmp_prcss_jdgmnt,
            "AppPlatform" as as_cmp_app_pltfrm,
            "AppID" as as_cmp_app_id
        from exptrans1
        where as_rec_or_comp = 'AsCompleted'
    ),
    recd as (
        select distinct
            "AppliedUncertainty" as appld_uncrtnty_typ,
            "ReportMeasuredAsDifference" as reprt_msrd_as_difference_ind,
            "ReportLimitsAsTolerance" as reprt_msrd_as_tolerance_ind,
            "NearSpecificationLimit" as near_spcfctn_lmt_ind,
            "NearGuardbandLimit" as near_gurdbd_lmt_ind,
            "Name" as cxkey_test_data_sctn_nm,
            "Title" as title,
            "ReportedJudgement" as rprtd_jdgmnt,
            "StandardJudgement" as std_jdgmnt,
            "GuardbandJudgement" as gurdbd_jdgmnt,
            "ProcessJudgement" as prcss_jdgmnt,
            null as insert_dt,
            null as upd_dt,
            "FLAG" as asrecdorcomp_data_ind,
            "CXKEY_ServiceOrderNumber" as cxkey_srvc_ord_num,
            "TestDataSectionID" as test_data_id,
            "CXKEY_LastModified" as last_modified_dt,
            "CXKEY_ProcedureID" as cxkey_cal_procedure_id,
            "CXKEY_ManufID" as cxkey_mfgr_id,
            "CXKEY_ManufModel" as cxkey_mfgr_mdl_num,
            "CXKEY_SerialNumber" as cxkey_sr_num,
            "CXKEY_AppFramework_FrameworkID" as cxkey_frmwrk_id,
            "CXKEY_SubmittingEntity" as cxkey_submitting_entity,
            "AppPlatform" as cxkey_app_platform,
            "AppID" as cxkey_app_id,
            "Name" as cxkey_cal_test_reslt_nm,
            "AS_RECEIVED_RESULTSTYPE" as cxkey_cal_test_reslt_typ,
            "ResultsType" as reslt_typ_datais_copy,
            TRY_TO_NUMBER("IsCharacterizationData") as is_characterization_data,
            "AsReceivedReportedJudgement" as cal_tst_rprtd_jdg
        from exptrans1
        where as_rec_or_comp = 'AsReceived'
    ),
    joiner as (
        select distinct stg1.*, stg2.*
        from recd stg1
        inner join
            compl stg2 on stg1.cxkey_test_data_sctn_nm = stg2.as_cmp_tst_data_sctn_nm
    -- and stg1.TEST_DATA_ID=stg2.AS_CMP_TST_DATA_SEC_ID
    ),
    lkptrans as (
        select distinct stg.cal_test_data_typ_id, raw_stg.*
        from joiner raw_stg
        left outer join
            ca_asrecdorcomp_data_typ_stg stg
            on stg.cxkey_test_nm = raw_stg.cxkey_test_data_sctn_nm
            and stg.cxkey_srvc_ord_num = raw_stg.cxkey_srvc_ord_num
            and stg.last_modified_dt = raw_stg.last_modified_dt
            and stg.cxkey_cal_procedure_id = raw_stg.cxkey_cal_procedure_id
            and stg.cxkey_mfgr_id = raw_stg.cxkey_mfgr_id
            and stg.cxkey_mfgr_mdl_num = raw_stg.cxkey_mfgr_mdl_num
            and stg.cxkey_sr_num = raw_stg.cxkey_sr_num
            and stg.cxkey_frmwrk_id = raw_stg.cxkey_frmwrk_id
            and stg.cxkey_submitting_entity = raw_stg.cxkey_submitting_entity
    ),
    final as (

        select *, ca_test_data_sctn_stg_seq.nextval as test_data_sctn_id from lkptrans
    )
select distinct *
from final
