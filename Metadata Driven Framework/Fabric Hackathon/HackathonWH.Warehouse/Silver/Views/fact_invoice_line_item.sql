-- --------------------------------------------------------------------------
-- 2.  INVOICE LINE ITEMS  –  Snapshot Fact (child grain)
-- --------------------------------------------------------------------------
CREATE   VIEW Silver.fact_invoice_line_item AS
SELECT
    CONVERT(
        VARCHAR(64),
        HASHBYTES('SHA2_256',
            ISNULL(entry_resource_id,'') + '|' +
            CAST(ISNULL(entry_resource_lineitem_sequence, 0) AS VARCHAR) + '|' +
            ISNULL(entry_resource_meta_lastupdated,'')
        ), 2
    )                                                          AS invoice_line_sk,

    -- ── FK to invoice header ──────────────────────────────────────────────
    entry_resource_id                                          AS invoice_id,
    entry_resource_lineitem_sequence                           AS line_item_sequence,

    -- ── FK to Claim item (ChargeItem) ─────────────────────────────────────
    REPLACE(entry_resource_lineitem_chargeitemreference_reference,
            'ChargeItem/', '')                                 AS charge_item_id,

    -- ── Product / service code (when coded, not by reference) ─────────────
    entry_resource_lineitem_chargeitemcodeableconcept_coding_code
                                                               AS charge_item_code,
    entry_resource_lineitem_chargeitemcodeableconcept_coding_system
                                                               AS charge_item_system,

    -- ── Price component ───────────────────────────────────────────────────
    --   type: 'base' | 'surcharge' | 'deduction' | 'discount' | 'tax' | 'informational'
    entry_resource_lineitem_pricecomponent_type                AS price_component_type,
    entry_resource_lineitem_pricecomponent_code_coding_code    AS price_component_code,
    entry_resource_lineitem_pricecomponent_code_coding_system  AS price_component_system,
    entry_resource_lineitem_pricecomponent_amount_value        AS price_component_amount,
    entry_resource_lineitem_pricecomponent_amount_currency     AS price_component_currency,

    -- ── Participants ──────────────────────────────────────────────────────
    entry_resource_participant_role_coding_code                AS participant_role_code,
    entry_resource_participant_role_coding_display             AS participant_role_display,
    entry_resource_participant_actor_display                   AS participant_actor_display,
    entry_resource_participant_actor_identifier_value          AS participant_actor_id,
    entry_resource_participant_actor_type                      AS participant_actor_type,

    -- ── Metadata ──────────────────────────────────────────────────────────
    entry_resource_meta_lastupdated                            AS meta_lastupdated,
    GETUTCDATE()                                               AS Silver_loaded_at
FROM HackathonLh.dbo.Invoice
WHERE entry_resource_id              IS NOT NULL
  AND entry_resource_lineitem_sequence IS NOT NULL;

GO