-- =============================================================================
-- FILE    : 04_Silver_organization_scd2.sql
-- LAYER   : Silver
-- TABLE   : dbo.Organization  (Bronze)
-- TARGET  : Silver.dim_organization  (SCD Type 2)
--
-- WHY SCD2 ?
--   Organization is a reference / dimension entity.  Org names, addresses,
--   types and contact details change infrequently but must be tracked so
--   Claims and Invoices can reflect the org attributes at time of service.
--
-- KEY COLUMNS USED
--   Business key : entry_resource_id
--   Descriptive  : name, alias, type_*, address_*, telecom_*, contact_*
--   Hierarchy    : partof_reference  (self-referencing hierarchy)
--   Metadata     : meta_lastupdated, meta_versionid
-- =============================================================================

-- --------------------------------------------------------------------------
-- 1.  STAGING VIEW
-- --------------------------------------------------------------------------
CREATE   VIEW Silver.stg_organization AS
SELECT
    -- ── Business Key ──────────────────────────────────────────────────────
    entry_resource_id                                          AS org_id,

    -- ── Core identity ─────────────────────────────────────────────────────
    entry_resource_name                                        AS org_name,
    entry_resource_alias                                       AS org_alias,
    entry_resource_active                                      AS is_active,
    entry_resource_language                                    AS language,

    -- ── Type ──────────────────────────────────────────────────────────────
    entry_resource_type_coding_code                            AS type_code,
    entry_resource_type_coding_display                         AS type_display,
    entry_resource_type_coding_system                          AS type_system,
    entry_resource_type_text                                   AS type_text,

    -- ── Address ───────────────────────────────────────────────────────────
    entry_resource_address_use                                 AS address_use,
    entry_resource_address_type                                AS address_type,
    entry_resource_address_text                                AS address_text,
    entry_resource_address_line                                AS address_line,
    entry_resource_address_city                                AS address_city,
    entry_resource_address_district                            AS address_district,
    entry_resource_address_state                               AS address_state,
    entry_resource_address_postalcode                          AS address_postalcode,
    entry_resource_address_country                             AS address_country,

    -- ── Telecom ───────────────────────────────────────────────────────────
    entry_resource_telecom_system                              AS telecom_system,
    entry_resource_telecom_use                                 AS telecom_use,
    entry_resource_telecom_value                               AS telecom_value,
    entry_resource_telecom_rank                                AS telecom_rank,

    -- ── Contact person ────────────────────────────────────────────────────
    entry_resource_contact_name_text                           AS contact_name_text,
    entry_resource_contact_name_given                          AS contact_name_given,
    entry_resource_contact_name_family                         AS contact_name_family,
    entry_resource_contact_name_prefix                         AS contact_name_prefix,
    entry_resource_contact_name_use                            AS contact_name_use,
    entry_resource_contact_purpose_coding_code                 AS contact_purpose_code,
    entry_resource_contact_purpose_coding_display              AS contact_purpose_display,
    entry_resource_contact_telecom_system                      AS contact_telecom_system,
    entry_resource_contact_telecom_use                         AS contact_telecom_use,
    entry_resource_contact_telecom_value                       AS contact_telecom_value,

    -- ── Identifiers ───────────────────────────────────────────────────────
    entry_resource_identifier_system                           AS identifier_system,
    entry_resource_identifier_value                            AS identifier_value,
    entry_resource_identifier_use                              AS identifier_use,
    entry_resource_identifier_type_coding_code                 AS identifier_type_code,
    entry_resource_identifier_type_coding_display              AS identifier_type_display,

    -- ── Hierarchy ─────────────────────────────────────────────────────────
    entry_resource_partof_display                              AS parent_org_display,
    -- Extract the id portion from "Organization/xxxx"
    REPLACE(entry_resource_partof_reference, 'Organization/', '')
                                                               AS parent_org_id,

    -- ── Metadata ──────────────────────────────────────────────────────────
    entry_resource_meta_lastupdated                            AS meta_lastupdated,
    entry_resource_meta_source                                 AS meta_source,
    entry_resource_meta_versionid                              AS meta_versionid,
    entry_resource_meta_profile                                AS meta_profile,
    entry_resource_resourcetype                                AS resource_type,
    entry_search_mode                                          AS search_mode,
    entry_fullurl                                              AS full_url
FROM HackathonLh.dbo.Organization
WHERE entry_resource_id IS NOT NULL;

GO