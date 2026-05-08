{{
    config(
        materialized = 'table',
        schema       = 'Silver'
    )
}}

SELECT 
    [claim_id],
    [diagnosis_sequence],
    [diagnosis_code],
    [diagnosis_display],
    [diagnosis_system],
    [diagnosis_type_code],
    [diagnosis_type_system],
    [condition_id],

    {{ to_datetime2('meta_lastupdated') }} AS [meta_lastupdated]

FROM {{ source('Silver', 'fact_claim_diagnosis') }}