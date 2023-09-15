{{
    config(
        materialized="incremental", unique_key=["cxkey_mfgr_mdl_num", "cxkey_mfgr_id", "cxkey_sr_num", "CXKEY_CAL_TEST_RESLT_NM", "EQUIP_SR_NUM", "EQUIP_ROLE", "EQUIP_MFGR_MDL_NUM", "CXKEY_CAL_TEST_RESLT_TYP", "EQPMNT_ID"]
    )
}}

with
    ca_asrecdorcomp_equip_used_raw_stg as (
        select distinct * from ca_asrecdorcomp_equip_used_raw
    ),
    exp_recd as (
        select distinct
            *,
            'AS_RECEIVED' as as_received_resultstype,
            null as as_completed_resultstype
        from ca_asrecdorcomp_equip_used_raw_stg
        where "ResultsType" = 'AS_RECEIVED'
    ),
    exp_cmpl as (
        select distinct
            *,
            null as as_received_resultstype,
            'AS_COMPLETED' as as_completed_resultstype
        from ca_asrecdorcomp_equip_used_raw_stg
        where "ResultsType" = 'AS_COMPLETED'
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
            "ResultsType",
            as_received_resultstype,
            as_completed_resultstype,
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
            ) as v_flag,
            "Name",
            "CXKEY_ProcedureID" as procedureid,
            "CXKEY_ManufID" as manufid,
            "CXKEY_ManufModel" as manufmodel,
            "CXKEY_SerialNumber" as serialnumber,
            "CXKEY_AppFramework_FrameworkID" as frameworkid,
            "CXKEY_SubmittingEntity" as submittingentity,
            "CXKEY_LastModified" as lastmodified,
            "CXKEY_ServiceOrderNumber" as serviceordernumber,
            "CXKEY_AppID" as appid,
            "CXKEY_AppPlatform" as appplatform,
            "ManufID",
            "ManufModel",
            "Role",
            "SerialNumber",
            "Manuf",
            "SetupName",
            "InstalledOptions"
        from data1
    ),
    lkptrans1 as (
        select distinct stg.eqpmnt_id, raw_stg.*
        from exptrans1 raw_stg
        left outer join
            ca_eqpmnt_stg stg
            on raw_stg."ManufID" = stg.mfgr_id
            and raw_stg."ManufModel" = stg.mfgr_mdl_num
            and raw_stg."SerialNumber" = stg.sr_num
    ),
    lkptrans as (
        select distinct stg.cal_test_data_typ_id, stg.asrecdorcomp_data_ind, raw_stg.*
        from lkptrans1 raw_stg
        left outer join
            ca_asrecdorcomp_data_typ_stg stg
            on stg.cxkey_test_nm = raw_stg."Name"
            and stg.cxkey_reslt_typ = raw_stg."ResultsType"
            and stg.cxkey_app_id = raw_stg.appid
            and stg.cxkey_app_platform = raw_stg.appplatform
            and stg.cxkey_srvc_ord_num = raw_stg.serviceordernumber
            and stg.last_modified_dt = raw_stg.lastmodified
            and stg.cxkey_cal_procedure_id = raw_stg.procedureid
            and stg.cxkey_mfgr_id = raw_stg.manufid
            and stg.cxkey_mfgr_mdl_num = raw_stg.manufmodel
            and stg.cxkey_sr_num = raw_stg.serialnumber
            and stg.cxkey_frmwrk_id = raw_stg.frameworkid
            and stg.cxkey_submitting_entity = raw_stg.submittingentity
    ),
    final as (
        select distinct
            eqpmnt_id as eqpmnt_id,
            cal_test_data_typ_id as cal_test_data_typ_id,
            null as insert_dt,
            null as upd_dt,
            asrecdorcomp_data_ind as asrecdorcomp_data_ind,
            serviceordernumber as cxkey_srvc_ord_num,
            lastmodified as last_modified_dt,
            "Name" as cxkey_cal_test_reslt_nm,
            "ResultsType" as cxkey_cal_test_reslt_typ,
            procedureid as cxkey_cal_procedure_id,
            manufid as cxkey_mfgr_id,
            manufmodel as cxkey_mfgr_mdl_num,
            serialnumber as cxkey_sr_num,
            frameworkid as cxkey_frmwrk_id,
            submittingentity as cxkey_submitting_entity,
            appplatform as cxkey_app_platform,
            appid as cxkey_app_id,
            "ManufID" as equip_mfgr_id,
            "Manuf" as equip_mfgr_nm,
            "ManufModel" as equip_mfgr_mdl_num,
            "SerialNumber" as equip_sr_num,
            "InstalledOptions" as equip_inst_opt,
            "SetupName" as equip_setup,
            "Role" as equip_role
        from lkptrans
    )
select distinct *
from final
