{{
    config(
        materialized = 'table',
        schema       = 'Silver'
    )
}}

SELECT DISTINCT
    [invoice_line_sk],
    [invoice_id],
    [line_item_sequence],
    [charge_item_id],
    [charge_item_code],
    [charge_item_system],
    [price_component_type],
    [price_component_code],
    [price_component_system],
    [price_component_amount],
    [price_component_currency],
    [participant_role_code],
    [participant_role_display],
    [participant_actor_display],
    [participant_actor_id],
    [participant_actor_type],

    {{ to_datetime2('meta_lastupdated') }}   AS [meta_lastupdated],
    {{ to_datetime2('Silver_loaded_at') }}   AS [silver_loaded_at]

FROM {{ source('Silver', 'fact_invoice_line_item') }}