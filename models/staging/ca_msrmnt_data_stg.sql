{{
    config(
        materialized='table'
    )
}}

with
    ca_msrmnt_data_raw_stg as (select * from ca_msrmnt_data_raw),
    exp_recd as (
        select
            *,
            'AS_RECEIVED' as as_received_resultstype,
            null as as_completed_resultstype
        from ca_msrmnt_data_raw_stg
        where "ResultsType" = 'AS_RECEIVED'
        
    ),
    exp_cmpl as (
        select
            *,
            null as as_received_resultstype,
            'AS_COMPLETED' as as_completed_resultstype
        from ca_msrmnt_data_raw_stg
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
        select
            null as cal_tst_rprtd_jdgmnt,
            "AppPlatform" as as_cmp_appplatform,
            "AppID" as as_cmp_app_id,
            "TestDataSectionID" as as_cmp_tst_data_sec_id,
            "MeasResultID" as as_cmp_meas_rslt_id,
            "DataID" as as_cmp_data_id,
            "ProcessJudgement" as as_cmp_prcss_jdgmnt,
            "ShortDataID" as as_cmp_shrt_data_id,
            "NearSpecificationLimit" as as_cmp_nr_gurdbd_lmt,
            "NearGuardbandLimit" as as_cmp_nr_spcfctn_lmt,
            "GuardbandJudgement" as as_cmp_gurdbd_judgmnt,
            "LimitsType" as as_cmp_limits_type,
            "StandardJudgement" as as_cmp_stndrd_judgmnt,
            "Units" as as_cmp_units,
            "UnitMultiplier" as as_cmp_unt_mult,
            "TestCondition" as as_cmp_tst_cnd,
            "Condition2" as as_cmp_cnd,
            "LowerLimit" as as_cmp_lwr_lmt,
            "LowerGuardbandLimit" as as_cmp_lwr_gurdbd_lmt,
            "LowerGuardbandAdjustLimit" as as_cmp_lwr_gurdbd_adj_lmt,
            "LowerProcessLimit" as as_cmp_lwr_prcss_lmt,
            null as as_cmp_lwr_prcss_adj_lmt,
            "Expected" as as_cmp_exp,
            "Measured" as as_cmp_msrd,
            "Normalized" as as_cmp_prcnt_of_spcfctn,
            "PercentOfSpecification" as as_cmp_nmr,
            "AdjustedCoverageFactor" as as_cmp_adj_covg_fctr,
            "TestUncertaintyRatio" as as_cmp_tst_uncrtnty_ratio,
            "MeasuredRawValue" as as_cmp_msrd_raw_val,
            "UpperLimit" as as_cmp_upr_lmt,
            "UpperGuardbandLimit" as as_cmp_upr_gurdbd_lmt,
            "UpperGuardbandAdjustLimit" as as_cmp_upr_gurdbd_adj_lmt,
            "UpperProcessLimit" as as_cmp_upr_prcss_lmt,
            "UpperProcessAdjustLimit" as as_cmp_upr_prcss_adj_lmt,
            "Uncertainty" as as_cmp_uncrtnty,
            "Uncertainty_Name" as as_cmp_uncrtnty_nm,
            "MetrologyDatabase" as as_cmp_metrology_db,
            "ProcessUncertainty" as as_cmp_prcss_uncrtnty,
            "ProcessUncertainty_Name" as as_cmp_prcss_uncrtnty_nm,
            "Guardband_Value" as as_cmp_gurdbd_val,
            "Guardband_Value_Type" as as_cmp_gurdbd_val_typ,
            "Guardband_AdjustmentValue" as as_cmp_adj_val,
            "Guardband_AdjustmentValue_Type" as as_cmp_adj_val_typ,
            "ProcessGuardband_Value" as as_cmp_prcss_gurdbd_val,
            "ProcessGuardband_Value_Type" as as_cmp_prcss_gurdbd_val_typ,
            "ProcessGuardband_AdjustmentValue" as as_cmp_prcss_gurdbd_adj_val,
            "ProcessGuardband_AdjustmentValue_Type" as as_cmp_prcss_grdbd_adj_val_typ,
            "NumMeasSamples" as as_cmp_num_meas_samples,
            "ImpliedLowerLimit" as as_cmp_implied_lwr_lmt,
            "ImpliedUpperLimit" as as_cmp_implied_upr_lmt,
            "Offset" as as_cmp_offset,
            "TestDataSectName" as as_cmp_cal_tst_data_sctn_nm
        from exptrans1
        where "ResultsType" = 'AS_COMPLETED'
    ),
    recd as (
        select distinct
            "Units" as unt,
            "UnitMultiplier" as unt_multiplier,
            "TestCondition" as test_condition,
            "Condition2" as condition_2,
            "LowerLimit" as lwr_lmt,
            "LowerGuardbandLimit" as lwr_gurdbd_lmt,
            "LowerGuardbandAdjustLimit" as lwr_gurdbd_adjust_lmt,
            "LowerProcessLimit" as lwr_prcss_lmt,
            null as lwr_prcss_adjust_lmt,
            "Expected" as expt,
            "Measured" as msrd,
            "Normalized" as msrmnt_nrmlzd,
            "PercentOfSpecification" as msrmnt_percnt_of_spcfctn,
            "AdjustedCoverageFactor" as msrmnt_coverage_factor,
            "TestUncertaintyRatio" as msrmnt_test_uncrtnty_ratio,
            "MeasuredRawValue" as msrd_raw_val,
            "UpperLimit" as upr_lmt,
            "UpperGuardbandLimit" as upr_gurdbd_lmt,
            "UpperGuardbandAdjustLimit" as upr_gurdbd_adjust_lmt,
            "UpperProcessLimit" as upr_prcss_lmt,
            "UpperProcessAdjustLimit" as upr_prcss_adjust_lmt,
            "Uncertainty" as uncrtnty,
            "Uncertainty_Name" as uncrtnty_nm,
            "MetrologyDatabase" as uncrtnty_metrology_database,
            "ProcessUncertainty" as prcss_uncrtnty,
            "ProcessUncertainty_Name" as prcss_uncrtnty_nm,
            "Guardband_Value" as gurdbd_val,
            "Guardband_Value_Type" as gurdbd_val_typ,
            "Guardband_AdjustmentValue" as gurdbd_adj_val,
            "Guardband_AdjustmentValue_Type" as gurdbd_adj_val_typ,
            "ProcessGuardband_Value" as prcss_gurdbd_val,
            "ProcessGuardband_Value_Type" as prcss_gurdbd_val_typ,
            "ImpliedLowerLimit" as implied_lwr_lmt,
            "ImpliedUpperLimit" as implied_upr_lmt,
            "Offset" as offset,
            "DataID" as data_id,
            "ShortDataID" as short_data_id,
            "LimitsType" as lmts_typ,
            "NearSpecificationLimit" as near_spcfctn_lmt_ind,
            "NearGuardbandLimit" as near_gurdbd_lmt_ind,
            "NumMeasSamples" as num_of_msrmnt_sample,
            "StandardJudgement" as std_jdgmnt,
            "GuardbandJudgement" as gurdbd_jdgmnt,
            "ProcessJudgement" as prcss_jdgmnt,
            null as insert_dt,
            null as upd_dt,
            "MeasResultID" as msrmnt_id,
            "CXKEY_ServiceOrderNumber" as cxkey_srvc_ord_num,
            "CXKEY_LastModified" as last_modified_dt,
            "CXKEY_ProcedureID" as cxkey_cal_procedure_id,
            "CXKEY_ManufID" as cxkey_mfgr_id,
            "CXKEY_ManufModel" as cxkey_mfgr_mdl_num,
            "CXKEY_SerialNumber" as cxkey_sr_num,
            "CXKEY_AppFramework_FrameworkID" as cxkey_frmwrk_id,
            "CXKEY_SubmittingEntity" as cxkey_submitting_entity,
            "AppPlatform" as cxkey_app_platform,
            "AppID" as cxkey_app_id,
            "TestName" as cxkey_cal_test_reslt_nm,
            "TestDataSectName" as cxkey_test_data_sctn_nm,
            "ResultsType" as cxkey_cal_test_reslt_typ,
            "ProcessGuardband_AdjustmentValue" as prcss_gurdbd_adj_val,
            "ProcessGuardband_AdjustmentValue_Type" as prcss_gurdbd_adj_val_typ,
            "TestDataSectionID" as cxkey_test_data_sctn_unique_id,
            "FLAG" as asrecdorcomp_data_ind
        from exptrans1
        where "ResultsType" = 'AS_RECEIVED'
    ),
    final as (
        select stg1.*, stg2.*, ca_msrmnt_data_stg_seq.nextval as msrmnt_data_indntifier
        from recd stg1
        left join
            compl stg2
            on stg1.cxkey_test_data_sctn_nm = stg2.as_cmp_cal_tst_data_sctn_nm
            and stg1.data_id = stg2.as_cmp_data_id
    )
select distinct *
from final
