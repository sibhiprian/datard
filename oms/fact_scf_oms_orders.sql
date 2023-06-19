{{
    config (
        pre_hook = before_begin("{{oms_audit_tbl_insert(1,'fact_scf_oms_orders') }}"),
        materialized = 'incremental', unique_key = '_id_oid',
        post_hook= after_commit("{{ yb_audit_control_update('yb_etl_ops','oms_audit_control_table','oms_staging','STG_OMS_BASE_INCR')  }};
                                 {{ yb_audit_control_update('yb_etl_ops','oms_audit_control_table','oms_staging','STG_SCF_OMS_STAGES_INCR')  }}")
        )
}}



  SELECT
  base._id_oid,
  base.borrower_id,
  base.borrower_name,
  base.current_step,
  base.deal_deleted,
  base.deal_id,
  base.deal_name,
  base.ei_id,
  base.lender_id,
  base.lender_name,
  base.product,
  base.sub_product,
  base.state,
  base.sub_state,
  base.state_machine_id,
  base.order_name,
  base.product_model_name,
  base.order_deleted,
  base.version,
  base.created_by,
  base.updated_by,
  base.created_at,
  base.updated_at,
  stages.ei_sent_date,
  stages.requirement_complete_date,
  stages.sender_cibil_approval_date,
  stages.lender_cibil_upload_date,
  stages.program_creation_date,
  stages.program_publish_date,
  stages.program_terms_creation_date,
  stages.mou_upload_date,
  stages.sanction_confirm_date,
  stages.cp_shortlist_date,
  stages.borrower_cibil_upload_date,
  stages.ei_approval_date,
  stages.registration_complete_date,
  stages.document_upload_date,
  stages.query_resolution_date
  FROM {{ref('stg_oms_base_incr') }} base inner join {{ref('stg_scf_oms_stages_incr') }} stages
  on base._id_oid = stages._id_oid
  where lower(base.product) = 'scf'
  ;
