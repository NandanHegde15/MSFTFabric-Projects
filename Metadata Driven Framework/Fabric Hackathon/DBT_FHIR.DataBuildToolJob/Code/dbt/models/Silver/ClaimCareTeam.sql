{{
    config(
        materialized = 'table',
        schema       = 'Silver'
    )
}}

SELECT 
    [claim_id],
    [care_team_sequence],
    [provider_id],
    [provider_display],
    [provider_identifier],
    [role_code],
    [role_display],

    {{ to_datetime2('meta_lastupdated') }} AS [meta_lastupdated]

FROM {{ source('Silver', 'fact_claim_care_team') }}