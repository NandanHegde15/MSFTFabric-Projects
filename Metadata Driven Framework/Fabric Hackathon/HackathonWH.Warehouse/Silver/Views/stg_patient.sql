-- =============================================================================
-- FILE    : 03_Silver_patient_scd2.sql
-- LAYER   : Silver
-- TABLE   : dbo.Patient  (Bronze)
-- TARGET  : Silver.dim_patient  (SCD Type 2)
--
-- WHY SCD2 ?
--   Patient is a slowly-changing dimension – demographic attributes (address,
--   name, phone, gender) can change over time.  We want full history so that
--   downstream Claim / Coverage analysis can join back to the patient state
--   that was in effect at the time of the event.
--
-- KEY COLUMNS USED
--   Business key : entry_resource_id        (FHIR Patient.id)
--   Descriptive  : name_family, name_given, gender, birthdate
--                  address_*, telecom_*
--   Metadata     : entry_resource_meta_lastupdated  (row freshness)
--
-- SCD2 MECHANICS (implemented as a CREATE-OR-ALTER VIEW + helper objects)
--   • effective_start_date  = meta_lastupdated of the source row
--   • effective_end_date    = lead(meta_lastupdated) - 1 day  (NULL = current)
--   • is_current            = 1 when effective_end_date IS NULL
--   • surrogate_key         = HASHBYTES('SHA2_256', id + effective_start_date)
-- =============================================================================

-- --------------------------------------------------------------------------
-- 1.  STAGING VIEW  –  de-duplicate within the bronze table
--     (Bronze tables are often flattened / repeated; pick the latest row
--      per patient id so SCD2 history tracks real changes only.)
-- --------------------------------------------------------------------------
CREATE   VIEW Silver.stg_patient AS
SELECT
    -- ── Business Key ──────────────────────────────────────────────────────
    entry_resource_id                                          AS patient_id,

    -- ── Demographics ──────────────────────────────────────────────────────
    entry_resource_name_family                                 AS name_family,
    entry_resource_name_given                                  AS name_given,
    entry_resource_gender                                      AS gender,
    entry_resource_birthdate                                   AS birth_date,

    -- ── Address ───────────────────────────────────────────────────────────
    entry_resource_address_use                                 AS address_use,
    entry_resource_address_line                                AS address_line,
    entry_resource_address_city                                AS address_city,
    entry_resource_address_state                               AS address_state,
    entry_resource_address_postalcode                          AS address_postalcode,
    entry_resource_address_text                                AS address_text,

    -- ── Contact ───────────────────────────────────────────────────────────
    entry_resource_telecom_system                              AS telecom_system,
    entry_resource_telecom_use                                 AS telecom_use,
    entry_resource_telecom_value                               AS telecom_value,

    -- ── Resource text ─────────────────────────────────────────────────────
    entry_resource_text_div                                    AS text_div,
    entry_resource_text_status                                 AS text_status,

    -- ── Metadata ──────────────────────────────────────────────────────────
    entry_resource_meta_lastupdated                            AS meta_lastupdated,
    entry_resource_meta_source                                 AS meta_source,
    entry_resource_meta_versionid                              AS meta_versionid,
    entry_resource_resourcetype                                AS resource_type,
    entry_search_mode                                          AS search_mode,
    entry_fullurl                                              AS full_url,

    -- ── Row number for de-dup (latest version wins per id) ────────────────
    ROW_NUMBER() OVER (
        PARTITION BY entry_resource_id
        ORDER BY     entry_resource_meta_lastupdated DESC,
                     entry_resource_meta_versionid   DESC
    )                                                          AS row_num
FROM HackathonLh.dbo.Patient
WHERE entry_resource_id IS NOT NULL;

GO