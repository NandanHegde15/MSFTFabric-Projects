-- =============================================================================
-- FILE    : 05_Silver_coverage_scd2.sql
-- LAYER   : Silver
-- TABLE   : dbo.Coverage  (Bronze)
-- TARGET  : Silver.dim_coverage  (SCD Type 2)
--
-- WHY SCD2 ?
--   Insurance Coverage is a dimension attached to a Patient over a period.
--   Policy class, network, payor and period attributes evolve; SCD2 preserves
--   the exact coverage that was in force when a Claim was filed.
--
-- KEY COLUMNS USED
--   Business key : entry_resource_id                   (FHIR Coverage.id)
--   FK → Patient : entry_resource_beneficiary_reference / _display
--   FK → Org     : entry_resource_payor_reference / _display (the insurer)
--   FK → Patient : entry_resource_subscriber_reference       (plan subscriber)
--   Descriptive  : status, type_*, class_*, network, period_*, relationship_*
--   Metadata     : meta_lastupdated, meta_versionid
-- =============================================================================

CREATE   VIEW Silver.stg_coverage AS
SELECT
    -- ── Business Key ──────────────────────────────────────────────────────
    entry_resource_id                                          AS coverage_id,

    -- ── Foreign Keys ──────────────────────────────────────────────────────
    -- Patient (the covered person)
    REPLACE(entry_resource_beneficiary_reference, 'Patient/', '')
                                                               AS patient_id,
    entry_resource_beneficiary_display                         AS beneficiary_display,

    -- Subscriber (policy holder – may differ from beneficiary)
    REPLACE(entry_resource_subscriber_reference, 'Patient/', '')
                                                               AS subscriber_id,
    entry_resource_subscriber_display                          AS subscriber_display,
    entry_resource_subscriberid                                AS subscriber_member_id,

    -- Payor (insurer / organization)
    REPLACE(entry_resource_payor_reference, 'Organization/', '')
                                                               AS payor_org_id,
    entry_resource_payor_display                               AS payor_display,
    entry_resource_payor_identifier_system                     AS payor_identifier_system,
    entry_resource_payor_identifier_value                      AS payor_identifier_value,

    -- ── Coverage details ──────────────────────────────────────────────────
    entry_resource_status                                      AS coverage_status,
    entry_resource_order                                       AS coverage_order,
    entry_resource_network                                     AS network,

    -- Type
    entry_resource_type_coding_code                            AS type_code,
    entry_resource_type_coding_display                         AS type_display,
    entry_resource_type_coding_system                          AS type_system,
    entry_resource_type_text                                   AS type_text,

    -- Class (e.g. group, plan, subplan)
    entry_resource_class_type_coding_code                      AS class_type_code,
    entry_resource_class_type_coding_display                   AS class_type_display,
    entry_resource_class_value                                 AS class_value,
    entry_resource_class_name                                  AS class_name,

    -- Relationship to subscriber
    entry_resource_relationship_coding_code                    AS relationship_code,
    entry_resource_relationship_coding_display                 AS relationship_display,

    -- Coverage period
    entry_resource_period_start                                AS period_start,
    entry_resource_period_end                                  AS period_end,

    -- Identifiers
    entry_resource_identifier_system                           AS identifier_system,
    entry_resource_identifier_value                            AS identifier_value,
    entry_resource_identifier_use                              AS identifier_use,
    entry_resource_identifier_type_coding_code                 AS identifier_type_code,
    entry_resource_identifier_type_text                        AS identifier_type_text,

    -- ── Metadata ──────────────────────────────────────────────────────────
    entry_resource_meta_lastupdated                            AS meta_lastupdated,
    entry_resource_meta_source                                 AS meta_source,
    entry_resource_meta_versionid                              AS meta_versionid,
    entry_resource_meta_profile                                AS meta_profile,
    entry_resource_resourcetype                                AS resource_type,
    entry_search_mode                                          AS search_mode,
    entry_fullurl                                              AS full_url
FROM HackathonLh.dbo.Coverage
WHERE entry_resource_id IS NOT NULL;

GO