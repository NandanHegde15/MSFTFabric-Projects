-- --------------------------------------------------------------------------
-- 2.  SCD TYPE-2 VIEW
-- --------------------------------------------------------------------------
CREATE   VIEW Silver.dim_organization AS
WITH versioned AS (
    SELECT
        CONVERT(
            VARCHAR(64),
            HASHBYTES(
                'SHA2_256',
                ISNULL(org_id, '') + '|' + ISNULL(meta_lastupdated, '')
            ),
            2
        )                                                      AS org_sk,

        org_id,
        org_name,
        org_alias,
        is_active,
        language,

        type_code,
        type_display,
        type_system,
        type_text,

        address_use,
        address_type,
        address_text,
        address_line,
        address_city,
        address_district,
        address_state,
        address_postalcode,
        address_country,

        telecom_system,
        telecom_use,
        telecom_value,
        telecom_rank,

        contact_name_text,
        contact_name_given,
        contact_name_family,
        contact_purpose_code,
        contact_purpose_display,
        contact_telecom_value,

        identifier_system,
        identifier_value,
        identifier_type_code,
        identifier_type_display,

        parent_org_id,
        parent_org_display,

        -- SCD2 window
        CAST(meta_lastupdated AS DATE)                         AS effective_start_date,
        CAST(
            LEAD(meta_lastupdated) OVER (
                PARTITION BY org_id
                ORDER BY     meta_lastupdated
            )
            AS DATE
        )                                                      AS effective_end_date_raw,

        meta_lastupdated,
        meta_source,
        meta_versionid,
        meta_profile,
        resource_type,
        search_mode,
        full_url
    FROM Silver.stg_organization
)
SELECT
    org_sk,
    org_id,
    org_name,
    org_alias,
    is_active,
    language,
    type_code,
    type_display,
    type_system,
    type_text,
    address_use,
    address_type,
    address_text,
    address_line,
    address_city,
    address_district,
    address_state,
    address_postalcode,
    address_country,
    telecom_system,
    telecom_use,
    telecom_value,
    telecom_rank,
    contact_name_text,
    contact_name_given,
    contact_name_family,
    contact_purpose_code,
    contact_purpose_display,
    contact_telecom_value,
    identifier_system,
    identifier_value,
    identifier_type_code,
    identifier_type_display,
    parent_org_id,
    parent_org_display,

    -- SCD2
    effective_start_date,
    DATEADD(DAY, -1, effective_end_date_raw)                   AS effective_end_date,
    CASE WHEN effective_end_date_raw IS NULL THEN 1 ELSE 0 END AS is_current,

    -- Audit
    meta_lastupdated,
    meta_source,
    meta_versionid,
    meta_profile,
    resource_type,
    search_mode,
    full_url,
    GETUTCDATE()                                               AS Silver_loaded_at
FROM versioned;

GO