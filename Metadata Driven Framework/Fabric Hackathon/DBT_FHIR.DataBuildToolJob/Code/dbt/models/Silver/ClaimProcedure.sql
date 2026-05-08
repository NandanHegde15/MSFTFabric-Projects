{{
    config(
        materialized = 'table',
        schema       = 'Silver'
    )
}}

SELECT  
    [claim_id],
    [procedure_sequence],
    [procedure_code],
    [procedure_display],
    [procedure_system],
    [procedure_ref_id],

    {{ to_datetime2('procedure_date') }}   AS [procedure_date],
    {{ to_datetime2('meta_lastupdated') }} AS [meta_lastupdated]

FROM {{ source('Silver', 'fact_claim_procedure') }}