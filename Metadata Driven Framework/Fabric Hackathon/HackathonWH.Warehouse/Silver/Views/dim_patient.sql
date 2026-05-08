-- --------------------------------------------------------------------------
-- 2.  SCD TYPE-2 VIEW  –  full history with effective date windows
-- --------------------------------------------------------------------------
CREATE   VIEW Silver.dim_patient AS
WITH base AS (
    SELECT *
    FROM Silver.stg_patient
    -- In a real pipeline you'd remove the row_num filter and feed ALL versions;
    -- here we expose all distinct versions ordered by meta_lastupdated.
),
versioned AS (
    SELECT
        -- ── Surrogate key ─────────────────────────────────────────────────
        CONVERT(
            VARCHAR(64),
            HASHBYTES(
                'SHA2_256',
                ISNULL(patient_id, '') + '|' + ISNULL(meta_lastupdated, '')
            ),
            2
        )                                                      AS patient_sk,

        -- ── Business key ──────────────────────────────────────────────────
        patient_id,

        -- ── Descriptive attributes ────────────────────────────────────────
        name_family,
        name_given,
        CONCAT(ISNULL(name_given,''), ' ', ISNULL(name_family,''))
                                                               AS full_name,
        gender,
        birth_date,

        -- ── Address ───────────────────────────────────────────────────────
        address_use,
        address_line,
        address_city,
        address_state,
        address_postalcode,
        address_text,

        -- ── Contact ───────────────────────────────────────────────────────
        telecom_system,
        telecom_use,
        telecom_value,

        -- ── SCD2 date window ──────────────────────────────────────────────
        CAST(meta_lastupdated AS DATE)                         AS effective_start_date,
        CAST(
            LEAD(meta_lastupdated) OVER (
                PARTITION BY patient_id
                ORDER BY     meta_lastupdated
            )
            AS DATE
        )                                                      AS effective_end_date_raw,

        -- ── Metadata pass-through ─────────────────────────────────────────
        meta_lastupdated,
        meta_source,
        meta_versionid,
        resource_type,
        search_mode,
        full_url
    FROM base
)
SELECT
    patient_sk,
    patient_id,
    name_family,
    name_given,
    full_name,
    gender,
    birth_date,
    address_use,
    address_line,
    address_city,
    address_state,
    address_postalcode,
    address_text,
    telecom_system,
    telecom_use,
    telecom_value,

    -- SCD2 window
    effective_start_date,
    DATEADD(DAY, -1, effective_end_date_raw)                   AS effective_end_date,   -- NULL = current record
    CASE WHEN effective_end_date_raw IS NULL THEN 1 ELSE 0 END AS is_current,

    -- Audit
    meta_lastupdated,
    meta_source,
    meta_versionid,
    resource_type,
    search_mode,
    full_url,
    GETUTCDATE()                                               AS Silver_loaded_at
FROM versioned;

GO