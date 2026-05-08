-- =============================================================================
-- FILE    : 06_Silver_claim_fact.sql
-- LAYER   : Silver
-- TABLE   : dbo.Claim  (Bronze)
-- TARGET  : Silver.fact_claim  +  Silver.fact_claim_item  (Snapshot Fact)
--
-- WHY SNAPSHOT FACT (not SCD2) ?
--   Claim is a transactional / event entity.  A claim is filed, may be
--   adjusted, but the individual versions are immutable events.  We treat
--   each claim version as an immutable snapshot row (append-only) rather
--   than SCD2.  The latest version can be identified with is_latest = 1.
--
--   We split into TWO fact views:
--     • fact_claim       – one row per claim header
--     • fact_claim_item  – one row per claim line item
--
-- KEY COLUMNS
--   Claim header
--     Business key : entry_resource_id
--     FK → Patient : entry_resource_patient_reference
--     FK → Org     : entry_resource_provider_reference, insurer_reference, facility_reference
--     FK → Coverage: entry_resource_insurance_coverage_reference
--     Measures     : entry_resource_total_value / _currency
--     Dates        : billableperiod_start/end, created
--
--   Claim line item
--     FK → Claim   : entry_resource_id  (claim_id)
--     Sequence     : entry_resource_item_sequence
--     Measures     : item_net_value, item_unitprice_value, item_quantity_value
--     Coding       : item_productorservice_*, item_category_*, item_revenue_*
--     Dates        : item_serviceddate, item_servicedperiod_*
-- =============================================================================

-- --------------------------------------------------------------------------
-- 1.  CLAIM HEADER  –  Snapshot Fact
-- --------------------------------------------------------------------------
CREATE   VIEW Silver.fact_claim AS
WITH ranked AS (
    SELECT
        -- ── Business Key ──────────────────────────────────────────────────
        entry_resource_id                                      AS claim_id,

        -- ── FK → Dimension tables ─────────────────────────────────────────
        REPLACE(entry_resource_patient_reference,   'Patient/',      '')  AS patient_id,
        REPLACE(entry_resource_provider_reference,  'Practitioner/', '')  AS provider_id,
        entry_resource_provider_display                        AS provider_display,
        entry_resource_provider_identifier_system              AS provider_identifier_system,
        entry_resource_provider_identifier_value               AS provider_identifier_value,

        REPLACE(entry_resource_insurer_reference,   'Organization/', '')  AS insurer_org_id,
        entry_resource_insurer_display                         AS insurer_display,

        REPLACE(entry_resource_facility_reference,  'Location/', '')      AS facility_id,
        entry_resource_facility_display                        AS facility_display,

        -- Insurance (primary)
        REPLACE(entry_resource_insurance_coverage_reference, 'Coverage/', '')
                                                               AS coverage_id,
        entry_resource_insurance_coverage_display              AS coverage_display,
        entry_resource_insurance_focal                         AS insurance_focal,
        entry_resource_insurance_sequence                      AS insurance_sequence,
        entry_resource_insurance_identifier_system             AS insurance_identifier_system,
        entry_resource_insurance_identifier_value              AS insurance_identifier_value,

        -- Prescription (if any)
        entry_resource_prescription_reference                  AS prescription_reference,

        -- ── Claim header attributes ───────────────────────────────────────
        entry_resource_status                                  AS claim_status,
        entry_resource_use                                     AS claim_use,

        -- Type / sub-type
        entry_resource_type_coding_code                        AS type_code,
        entry_resource_type_coding_display                     AS type_display,
        entry_resource_type_text                               AS type_text,
        entry_resource_subtype_coding_code                     AS subtype_code,
        entry_resource_subtype_text                            AS subtype_text,

        -- Priority
        entry_resource_priority_coding_code                    AS priority_code,
        entry_resource_priority_coding_display                 AS priority_display,

        -- Payee
        entry_resource_payee_type_coding_code                  AS payee_type_code,

        -- ── Dates ─────────────────────────────────────────────────────────
        entry_resource_created                                 AS claim_created_date,
        entry_resource_billableperiod_start                    AS billable_period_start,
        entry_resource_billableperiod_end                      AS billable_period_end,

        -- ── Financial Measures ────────────────────────────────────────────
        entry_resource_total_value                             AS total_amount,
        entry_resource_total_currency                          AS total_currency,

        -- ── Identifiers ───────────────────────────────────────────────────
        entry_resource_identifier_system                       AS identifier_system,
        entry_resource_identifier_value                        AS identifier_value,

        -- ── Metadata ──────────────────────────────────────────────────────
        entry_resource_meta_lastupdated                        AS meta_lastupdated,
        entry_resource_meta_source                             AS meta_source,
        entry_resource_meta_versionid                          AS meta_versionid,
        entry_resource_resourcetype                            AS resource_type,
        entry_search_mode                                      AS search_mode,
        entry_fullurl                                          AS full_url,

        -- ── Version ranking (latest = 1) ──────────────────────────────────
        ROW_NUMBER() OVER (
            PARTITION BY entry_resource_id
            ORDER BY     entry_resource_meta_lastupdated DESC,
                         entry_resource_meta_versionid   DESC
        )                                                      AS version_rank
    FROM HackathonLh.dbo.Claim
    WHERE entry_resource_id IS NOT NULL
      AND (entry_resource_resourcetype = 'Claim'
           OR entry_resource_resourcetype IS NULL)
)
SELECT
    -- Surrogate key: claim + version snapshot
    CONVERT(
        VARCHAR(64),
        HASHBYTES('SHA2_256',
            ISNULL(claim_id,'') + '|' + ISNULL(meta_lastupdated,'')
        ), 2
    )                                                          AS claim_sk,

    claim_id,
    patient_id,
    provider_id,
    provider_display,
    provider_identifier_system,
    provider_identifier_value,
    insurer_org_id,
    insurer_display,
    facility_id,
    facility_display,
    coverage_id,
    coverage_display,
    insurance_focal,
    insurance_sequence,
    insurance_identifier_system,
    insurance_identifier_value,
    prescription_reference,
    claim_status,
    claim_use,
    type_code,
    type_display,
    type_text,
    subtype_code,
    subtype_text,
    priority_code,
    priority_display,
    payee_type_code,
    claim_created_date,
    billable_period_start,
    billable_period_end,
    total_amount,
    total_currency,
    identifier_system,
    identifier_value,

    -- Snapshot flags
    CASE WHEN version_rank = 1 THEN 1 ELSE 0 END              AS is_latest,
    version_rank,

    -- Audit
    meta_lastupdated,
    meta_source,
    meta_versionid,
    resource_type,
    search_mode,
    full_url,
    GETUTCDATE()                                               AS Silver_loaded_at
FROM ranked;

GO