{{
    config(
        materialized = 'table',
        schema       = 'Silver'
    )
}}

SELECT 
    [claim_item_sk],
    [claim_id],
    [item_sequence],
    [care_team_sequence],
    [diagnosis_sequence],
    [procedure_sequence],
    [information_sequence],
    [product_service_code],
    [product_service_display],
    [product_service_system],
    [product_service_text],
    [category_code],
    [category_display],
    [revenue_code],
    [revenue_code_system],
    [modifier_code],
    [modifier_system],
    [location_code],
    [location_display],
    [location_ref_display],
    [location_ref_identifier],
    [encounter_id],

    {{ to_datetime2('serviced_date') }}           AS [serviced_date],
    {{ to_datetime2('serviced_period_start') }}   AS [serviced_period_start],
    {{ to_datetime2('serviced_period_end') }}     AS [serviced_period_end],

    [quantity],
    [quantity_unit],
    [unit_price],
    [unit_price_currency],
    [net_amount],
    [net_currency],

    {{ to_datetime2('meta_lastupdated') }}        AS [meta_lastupdated],
    {{ to_datetime2('Silver_loaded_at') }}        AS [silver_loaded_at]

FROM {{ source('Silver', 'fact_claim_item') }}
WHERE claim_item_sk IS NOT NULL