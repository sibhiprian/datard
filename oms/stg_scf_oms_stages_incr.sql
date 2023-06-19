{{
    config (
        pre_hook= before_begin("{{ yb_audit_control_insert('yb_etl_ops','oms_audit_control_table','STG_SCF_OMS_STAGES_INCR','yb_mongo_flattened_source','oms_orders','updated_at','FACT_SCF_OMS_ORDERS','OMS')  }}")
        )
}}



select st._id_oid,
max(case when upper(st.state_info_state_name)  ='DISCOVERY' and st.state_info_sub_states_state_name ='EI_SENT' then from_unixtime(st.state_info_sub_states_completed_at) else null end) as ei_sent_date,
max(case when upper(st.state_info_state_name)  ='SETUP_REQUIREMENT' and st.state_info_sub_states_state_name ='REQUIREMENT_COMPLETE' then from_unixtime(st.state_info_sub_states_completed_at) else null end) as requirement_complete_date,
max(case when upper(st.state_info_state_name)  ='LENDER_CIBIL_CONSENT' and st.state_info_sub_states_state_name ='LENDER_CIBIL_APPROVAL' then from_unixtime(st.state_info_sub_states_completed_at) else null end) as lender_cibil_approval_date,
max(case when upper(st.state_info_state_name)  ='LENDER_CIBIL_CONSENT' and st.state_info_sub_states_state_name ='LENDER_CIBIL_UPLOAD' then from_unixtime(st.state_info_sub_states_completed_at) else null end) as lender_cibil_upload_date,
max(case when upper(st.state_info_state_name)  ='INITIATION' and st.state_info_sub_states_state_name ='PROGRAM_CREATION' then from_unixtime(st.state_info_sub_states_completed_at) else null end) as program_creation_date,
max(case when upper(st.state_info_state_name)  ='INITIATION' and st.state_info_sub_states_state_name ='PROGRAM_PUBLISH' then from_unixtime(st.state_info_sub_states_completed_at) else null end) as program_publish_date,
max(case when upper(st.state_info_state_name)  ='FINALIZATION' and st.state_info_sub_states_state_name ='PROGRAM_TERMS_CREATION' then from_unixtime(st.state_info_sub_states_completed_at) else null end) as program_terms_creation_date,
max(case when upper(st.state_info_state_name)  ='FINALIZATION' and st.state_info_sub_states_state_name ='MOU_UPLOAD' then from_unixtime(st.state_info_sub_states_completed_at) else null end) as mou_upload_date,
max(case when upper(st.state_info_state_name)  ='SCF_SANCTION' and st.state_info_sub_states_state_name ='SANCTION_CONFIRM' then from_unixtime(st.state_info_sub_states_completed_at) else null end) as sanction_confirm_date,
max(case when upper(st.state_info_state_name)  ='SETUP_REQUIREMENT' and st.state_info_sub_states_state_name ='CP_SHORTLIST' then from_unixtime(st.state_info_sub_states_completed_at) else null end) as cp_shortlist_date,
max(case when upper(st.state_info_state_name)  ='LENDER_CIBIL_CONSENT' and st.state_info_sub_states_state_name ='BORROWER_CIBIL_UPLOAD' then from_unixtime(st.state_info_sub_states_completed_at) else null end) as borrower_cibil_upload_date,
max(case when upper(st.state_info_state_name)  ='DISCOVERY' and st.state_info_sub_states_state_name ='EI_APPROVAL' then from_unixtime(st.state_info_sub_states_completed_at) else null end) as ei_approval_date,
max(case when upper(st.state_info_state_name)  ='SETUP_REQUIREMENT' and st.state_info_sub_states_state_name ='REGISTRATION_COMPLETE' then from_unixtime(st.state_info_sub_states_completed_at) else null end) as registration_complete_date,
max(case when upper(st.state_info_state_name)  ='SETUP_REQUIREMENT' and st.state_info_sub_states_state_name ='DOCUMENT_UPLOAD' then from_unixtime(st.state_info_sub_states_completed_at) else null end) as document_upload_date,
max(case when upper(st.state_info_state_name)  ='SCF_SANCTION' and st.state_info_sub_states_state_name ='QUERY_RESOLUTION' then from_unixtime(st.state_info_sub_states_completed_at) else null end) as query_resolution_date
from {{ source('yb_mongo_flattened_source','oms_orders') }} st
WHERE {{ yb_incremental_check('updated_at','STG_SCF_OMS_STAGES_INCR','yb_etl_ops','oms_audit_control_table') }}
group by st._id_oid
;
