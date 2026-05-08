{{
    config(
        materialized = 'table',
        schema       = 'Silver'
    )
}}

SELECT  
    [claim_sk],
    [claim_id],
    [patient_id],
    [provider_id],
    [provider_display],
    [provider_identifier_system],
    [provider_identifier_value],
    [insurer_org_id],
    [insurer_display],
    [facility_id],
    [facility_display],
    [coverage_id],
    [coverage_display],
    [insurance_focal],
    [insurance_sequence],
    [insurance_identifier_system],
    [insurance_identifier_value],
    [prescription_reference],
    [claim_status],
    [claim_use],
    [type_code],
    [type_display],
    [type_text],
    [subtype_code],
    [subtype_text],
    [priority_code],
    [priority_display],
    [payee_type_code],

    {{ to_datetime2('claim_created_date') }}      AS [claim_created_date],
    {{ to_datetime2('billable_period_start') }}  AS [billable_period_start],
    {{ to_datetime2('billable_period_end') }}    AS [billable_period_end],

    [total_amount],
    [total_currency],
    [identifier_system],
    [identifier_value],
    [is_latest],
    [version_rank],

    {{ to_datetime2('meta_lastupdated') }}       AS [meta_lastupdated],

    [meta_source],
    [meta_versionid],
    [resource_type],
    [search_mode],
    [full_url],

    {{ to_datetime2('Silver_loaded_at') }}       AS [silver_loaded_at]

FROM {{ source('Silver', 'fact_claim') }}
WHERE patient_id IS NOT NULL AND claim_sk IS NOT NULL AND [is_latest] = 1