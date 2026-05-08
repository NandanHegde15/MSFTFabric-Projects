{{
    config(
        materialized = 'table',
        schema       = 'Silver'
    )
}}

SELECT  
    [invoice_sk],
    [invoice_id],
    [patient_id],
    [patient_display],
    [recipient_display],
    [recipient_identifier],
    [recipient_type],
    [issuer_display],
    [issuer_identifier_system],
    [issuer_identifier_value],
    [issuer_type],
    [source_claim_id],
    [invoice_status],

    {{ to_datetime2('invoice_date') }}       AS [invoice_date],

    [identifier_system],
    [identifier_value],
    [identifier_type_code],
    [identifier_type_display],
    [total_gross_amount],
    [total_gross_currency],
    [total_net_amount],
    [total_net_currency],
    [note_text],

    {{ to_datetime2('note_time') }}          AS [note_time],

    [is_latest],
    [version_rank],

    {{ to_datetime2('meta_lastupdated') }}   AS [meta_lastupdated],

    [meta_source],
    [meta_versionid],
    [resource_type],
    [search_mode],
    [full_url],

    {{ to_datetime2('Silver_loaded_at') }}   AS [silver_loaded_at]

FROM {{ source('Silver', 'fact_invoice') }}
WHERE [is_latest] = 1 AND [patient_id] IS NOT NULL