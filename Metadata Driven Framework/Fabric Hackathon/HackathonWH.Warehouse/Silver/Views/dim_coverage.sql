-- --------------------------------------------------------------------------
-- SCD TYPE-2 VIEW
-- --------------------------------------------------------------------------
CREATE   VIEW Silver.dim_coverage AS
WITH versioned AS (
    SELECT
        CONVERT(
            VARCHAR(64),
            HASHBYTES(
                'SHA2_256',
                ISNULL(coverage_id, '') + '|' + ISNULL(meta_lastupdated, '')
            ),
            2
        )                                                      AS coverage_sk,

        coverage_id,
        patient_id,
        beneficiary_display,
        subscriber_id,
        subscriber_display,
        subscriber_member_id,
        payor_org_id,
        payor_display,
        payor_identifier_system,
        payor_identifier_value,
        coverage_status,
        coverage_order,
        network,
        type_code,
        type_display,
        type_system,
        type_text,
        class_type_code,
        class_type_display,
        class_value,
        class_name,
        relationship_code,
        relationship_display,
        period_start,
        period_end,
        identifier_system,
        identifier_value,
        identifier_type_code,
        identifier_type_text,

        -- SCD2 window
        CAST(meta_lastupdated AS DATE)                         AS effective_start_date,
        CAST(
            LEAD(meta_lastupdated) OVER (
                PARTITION BY coverage_id
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
    FROM Silver.stg_coverage
)
SELECT
    coverage_sk,
    coverage_id,
    patient_id,
    beneficiary_display,
    subscriber_id,
    subscriber_display,
    subscriber_member_id,
    payor_org_id,
    payor_display,
    payor_identifier_system,
    payor_identifier_value,
    coverage_status,
    coverage_order,
    network,
    type_code,
    type_display,
    type_system,
    type_text,
    class_type_code,
    class_type_display,
    class_value,
    class_name,
    relationship_code,
    relationship_display,
    period_start,
    period_end,
    identifier_system,
    identifier_value,
    identifier_type_code,
    identifier_type_text,

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