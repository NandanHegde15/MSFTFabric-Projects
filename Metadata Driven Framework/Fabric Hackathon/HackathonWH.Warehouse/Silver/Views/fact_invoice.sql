-- =============================================================================
-- FILE    : 07_Silver_invoice_fact.sql
-- LAYER   : Silver
-- TABLE   : dbo.Invoice  (Bronze)
-- TARGET  : Silver.fact_invoice  +  Silver.fact_invoice_line_item
--           Pattern  : Snapshot Fact  (same rationale as Claim)
--
-- WHY SNAPSHOT FACT (not SCD2) ?
--   An Invoice is the financial settlement document resulting from a Claim.
--   It is immutable once issued; any correction creates a new version.
--   Like Claim, we keep all versions and flag is_latest.
--
-- GRAIN
--   fact_invoice           – 1 row per invoice header version
--   fact_invoice_line_item – 1 row per invoice line × price component
--
-- KEY COLUMNS
--   Invoice header
--     Business key : entry_resource_id
--     FK → Patient : entry_resource_subject_reference
--     FK → Org     : entry_resource_issuer_*
--     FK → Claim   : entry_resource_extension_valuereference_reference
--                    (extension url ends in "claim")
--     Measures     : totalgross_value, totalnet_value
--     Date         : entry_resource_date
--
--   Line item
--     FK → Invoice : entry_resource_id
--     Sequence     : entry_resource_lineitem_sequence
--     Measures     : pricecomponent_amount_value
--     FK → Claim   : lineitem_chargeitemreference_reference
-- =============================================================================

-- --------------------------------------------------------------------------
-- 1.  INVOICE HEADER  –  Snapshot Fact
-- --------------------------------------------------------------------------
CREATE   VIEW Silver.fact_invoice AS
WITH ranked AS (
    SELECT
        -- ── Business Key ──────────────────────────────────────────────────
        entry_resource_id                                      AS invoice_id,

        -- ── FK → dimension / other facts ──────────────────────────────────
        -- Subject = Patient
        REPLACE(entry_resource_subject_reference, 'Patient/', '')
                                                               AS patient_id,
        entry_resource_subject_display                         AS patient_display,

        -- Recipient (payor / organisation)
        entry_resource_recipient_display                       AS recipient_display,
        entry_resource_recipient_identifier_value              AS recipient_identifier,
        entry_resource_recipient_type                          AS recipient_type,

        -- Issuer (the billing organisation)
        entry_resource_issuer_display                          AS issuer_display,
        entry_resource_issuer_identifier_system                AS issuer_identifier_system,
        entry_resource_issuer_identifier_value                 AS issuer_identifier_value,
        entry_resource_issuer_type                             AS issuer_type,

        -- Linked Claim reference (carried via FHIR extension)
        --   FHIR extension url pattern: "...claimReference" → extract claim id
        REPLACE(entry_resource_extension_valuereference_reference, 'Claim/', '')
                                                               AS source_claim_id,

        -- ── Invoice attributes ────────────────────────────────────────────
        entry_resource_status                                  AS invoice_status,
        entry_resource_date                                    AS invoice_date,

        -- Identifiers
        entry_resource_identifier_system                       AS identifier_system,
        entry_resource_identifier_value                        AS identifier_value,
        entry_resource_identifier_type_coding_code             AS identifier_type_code,
        entry_resource_identifier_type_coding_display          AS identifier_type_display,

        -- ── Financial Measures ────────────────────────────────────────────
        entry_resource_totalgross_value                        AS total_gross_amount,
        entry_resource_totalgross_currency                     AS total_gross_currency,
        entry_resource_totalnet_value                          AS total_net_amount,
        entry_resource_totalnet_currency                       AS total_net_currency,

        -- Notes
        entry_resource_note_text                               AS note_text,
        entry_resource_note_time                               AS note_time,

        -- ── Metadata ──────────────────────────────────────────────────────
        entry_resource_meta_lastupdated                        AS meta_lastupdated,
        entry_resource_meta_source                             AS meta_source,
        entry_resource_meta_versionid                          AS meta_versionid,
        entry_resource_resourcetype                            AS resource_type,
        entry_search_mode                                      AS search_mode,
        entry_fullurl                                          AS full_url,

        ROW_NUMBER() OVER (
            PARTITION BY entry_resource_id
            ORDER BY     entry_resource_meta_lastupdated DESC,
                         entry_resource_meta_versionid   DESC
        )                                                      AS version_rank
    FROM HackathonLh.dbo.Invoice
    WHERE entry_resource_id IS NOT NULL
)
SELECT
    CONVERT(
        VARCHAR(64),
        HASHBYTES('SHA2_256',
            ISNULL(invoice_id,'') + '|' + ISNULL(meta_lastupdated,'')
        ), 2
    )                                                          AS invoice_sk,

    invoice_id,
    patient_id,
    patient_display,
    recipient_display,
    recipient_identifier,
    recipient_type,
    issuer_display,
    issuer_identifier_system,
    issuer_identifier_value,
    issuer_type,
    source_claim_id,
    invoice_status,
    invoice_date,
    identifier_system,
    identifier_value,
    identifier_type_code,
    identifier_type_display,
    total_gross_amount,
    total_gross_currency,
    total_net_amount,
    total_net_currency,
    note_text,
    note_time,

    CASE WHEN version_rank = 1 THEN 1 ELSE 0 END              AS is_latest,
    version_rank,

    meta_lastupdated,
    meta_source,
    meta_versionid,
    resource_type,
    search_mode,
    full_url,
    GETUTCDATE()                                               AS Silver_loaded_at
FROM ranked;

GO