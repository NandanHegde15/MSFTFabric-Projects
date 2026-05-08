{{
    config(
        materialized = 'table',
        schema       = 'Gold'
    )
}}

/*
  gold_patient_360
  ────────────────
  One row per CURRENT patient.
  Enriched with:
    • Current (is_current=1) coverage details + payor org name
    • Claim counts and financial summary
    • Invoice payment summary
  Designed for Patient 360 dashboards and care-gap analysis.
*/

WITH current_patients AS (
    SELECT *
    FROM {{ ref('Patient') }}
),

current_coverage AS (
    SELECT
        patient_id,
        coverage_id,
        payor_org_id,
        payor_display,
        type_display                    AS coverage_type,
        class_name                      AS coverage_class,
        network,
        period_start                    AS coverage_start,
        period_end                      AS coverage_end,
        relationship_display            AS member_relationship,
        subscriber_member_id,
        -- Latest coverage per patient
        ROW_NUMBER() OVER (
            PARTITION BY patient_id
            ORDER BY     CAST(period_start AS DATE) DESC
        )                               AS coverage_rank
    FROM {{ ref('Coverage') }}
),

latest_coverage AS (
    SELECT * FROM current_coverage WHERE coverage_rank = 1
),

current_payor AS (
    SELECT org_id, org_name AS payor_org_name
    FROM {{ ref('Organization') }}
),

claim_summary AS (
    SELECT
        patient_id,
        COUNT(DISTINCT claim_id)            AS total_claims,
        SUM(total_amount)                   AS total_claimed_amount,
        MIN(CAST(claim_created_date AS DATE)) AS first_claim_date,
        MAX(CAST(claim_created_date AS DATE)) AS last_claim_date,
        COUNT(DISTINCT CASE WHEN claim_status = 'active' THEN claim_id END)
                                            AS active_claims,
        COUNT(DISTINCT CASE WHEN claim_status = 'cancelled' THEN claim_id END)
                                            AS cancelled_claims
    FROM {{ ref('Claim') }}
    GROUP BY patient_id
),

invoice_summary AS (
    SELECT
        patient_id,
        COUNT(DISTINCT invoice_id)          AS total_invoices,
        SUM(total_gross_amount)             AS total_gross_billed,
        SUM(total_net_amount)               AS total_net_billed,
        SUM(total_gross_amount - total_net_amount)
                                            AS total_discounts,
        MAX(CAST(invoice_date AS DATE))     AS last_invoice_date
    FROM {{ ref('Invoice') }}
    WHERE is_latest = 1
    GROUP BY patient_id
)

SELECT
    -- Patient identity
    p.patient_id,
    p.full_name,
    p.name_family,
    p.name_given,
    p.gender,
    p.birth_date,
    DATEDIFF(YEAR, CAST(p.birth_date AS DATE), CAST(GETUTCDATE() AS DATE))
                                            AS age_years,

    -- Address
    p.address_line,
    p.address_city,
    p.address_state,
    p.address_postalcode,

    -- Contact
    p.telecom_value                         AS phone_or_email,

    -- Coverage (current)
    cov.coverage_id,
    cov.coverage_type,
    cov.coverage_class,
    cov.network                             AS insurance_network,
    cov.coverage_start,
    cov.coverage_end,
    cov.member_relationship,
    cov.subscriber_member_id,
    cov.payor_display,
    org.payor_org_name,

    -- Claim KPIs
    ISNULL(cs.total_claims, 0)              AS total_claims,
    ISNULL(cs.total_claimed_amount, 0)      AS total_claimed_amount,
    cs.first_claim_date,
    cs.last_claim_date,
    ISNULL(cs.active_claims, 0)             AS active_claims,
    ISNULL(cs.cancelled_claims, 0)          AS cancelled_claims,

    -- Invoice KPIs
    ISNULL(inv.total_invoices, 0)           AS total_invoices,
    ISNULL(inv.total_gross_billed, 0)       AS total_gross_billed,
    ISNULL(inv.total_net_billed, 0)         AS total_net_billed,
    ISNULL(inv.total_discounts, 0)          AS total_discounts,
    inv.last_invoice_date,

    -- Derived metrics
    CASE
        WHEN ISNULL(inv.total_gross_billed, 0) = 0 THEN NULL
        ELSE ROUND(
            (inv.total_net_billed / NULLIF(inv.total_gross_billed, 0)) * 100,
        2)
    END                                     AS net_to_gross_pct,

    -- Audit
    {{ to_datetime2('p.meta_lastupdated') }}        AS patient_last_updated,
    {{ to_datetime2('GETUTCDATE()') }}        AS gold_loaded_at

FROM current_patients            p
LEFT JOIN latest_coverage        cov ON p.patient_id = cov.patient_id
LEFT JOIN current_payor          org ON cov.payor_org_id = org.org_id
LEFT JOIN claim_summary          cs  ON p.patient_id = cs.patient_id
LEFT JOIN invoice_summary        inv ON p.patient_id = inv.patient_id
