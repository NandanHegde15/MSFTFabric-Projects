-- --------------------------------------------------------------------------
-- 2.  CLAIM LINE ITEMS  –  Snapshot Fact (child grain)
-- --------------------------------------------------------------------------
CREATE   VIEW Silver.fact_claim_item AS
SELECT
    -- ── Surrogate key ─────────────────────────────────────────────────────
    CONVERT(
        VARCHAR(64),
        HASHBYTES('SHA2_256',
            ISNULL(entry_resource_id,'') + '|' +
            CAST(ISNULL(entry_resource_item_sequence, 0) AS VARCHAR) + '|' +
            ISNULL(entry_resource_meta_lastupdated,'')
        ), 2
    )                                                          AS claim_item_sk,

    -- ── FK to claim header ────────────────────────────────────────────────
    entry_resource_id                                          AS claim_id,
    entry_resource_item_sequence                               AS item_sequence,

    -- ── Sequences linking back to care-team / diagnosis / procedure ───────
    entry_resource_item_careteamsequence                       AS care_team_sequence,
    entry_resource_item_diagnosissequence                      AS diagnosis_sequence,
    entry_resource_item_proceduresequence                      AS procedure_sequence,
    entry_resource_item_informationsequence                    AS information_sequence,

    -- ── Product / Service coding ──────────────────────────────────────────
    entry_resource_item_productorservice_coding_code           AS product_service_code,
    entry_resource_item_productorservice_coding_display        AS product_service_display,
    entry_resource_item_productorservice_coding_system         AS product_service_system,
    entry_resource_item_productorservice_text                  AS product_service_text,

    -- ── Category & Revenue codes ──────────────────────────────────────────
    entry_resource_item_category_coding_code                   AS category_code,
    entry_resource_item_category_coding_display                AS category_display,
    entry_resource_item_revenue_coding_code                    AS revenue_code,
    entry_resource_item_revenue_coding_system                  AS revenue_code_system,

    -- ── Modifier ──────────────────────────────────────────────────────────
    entry_resource_item_modifier_coding_code                   AS modifier_code,
    entry_resource_item_modifier_coding_system                 AS modifier_system,

    -- ── Service location ──────────────────────────────────────────────────
    entry_resource_item_locationcodeableconcept_coding_code    AS location_code,
    entry_resource_item_locationcodeableconcept_coding_display AS location_display,
    entry_resource_item_locationreference_display              AS location_ref_display,
    entry_resource_item_locationreference_identifier_value     AS location_ref_identifier,

    -- ── Linked encounter ──────────────────────────────────────────────────
    REPLACE(entry_resource_item_encounter_reference, 'Encounter/', '')
                                                               AS encounter_id,

    -- ── Service dates ─────────────────────────────────────────────────────
    entry_resource_item_serviceddate                           AS serviced_date,
    entry_resource_item_servicedperiod_start                   AS serviced_period_start,
    entry_resource_item_servicedperiod_end                     AS serviced_period_end,

    -- ── Financial Measures ────────────────────────────────────────────────
    entry_resource_item_quantity_value                         AS quantity,
    entry_resource_item_quantity_unit                          AS quantity_unit,
    entry_resource_item_unitprice_value                        AS unit_price,
    entry_resource_item_unitprice_currency                     AS unit_price_currency,
    entry_resource_item_net_value                              AS net_amount,
    entry_resource_item_net_currency                           AS net_currency,

    -- ── Metadata ──────────────────────────────────────────────────────────
    entry_resource_meta_lastupdated                            AS meta_lastupdated,
    GETUTCDATE()                                               AS Silver_loaded_at
FROM HackathonLh.dbo.Claim
WHERE entry_resource_id         IS NOT NULL
  AND entry_resource_item_sequence IS NOT NULL;

GO