{{
    config(
        materialized = 'table',
        schema       = 'Gold'
    )
}}

/*
  gold_claim_summary
  ──────────────────
  One row per LATEST claim version.
  Fully denormalised for reporting – joins patient name, insurer org, facility,
  coverage network, diagnosis count, procedure count and line-item totals.
  Use this as the primary claim analysis table for BI tools.
*/

WITH latest_claims AS (
    SELECT * FROM {{ ref('Claim') }} 
),

patients AS (
    SELECT patient_id, full_name, gender, birth_date, address_city, address_state
    FROM   {{ ref('Patient') }}   
),

insurers AS (
    SELECT org_id, org_name AS insurer_name, address_city AS insurer_city,
           address_state AS insurer_state
    FROM   {{ ref('Organization') }}     
),

coverage AS (
    SELECT coverage_id, type_display AS coverage_type, network,
           class_name AS plan_class, payor_display
    FROM   {{ ref('Coverage') }}         
),

-- Aggregate line items per claim
item_totals AS (
    SELECT
        claim_id,
        COUNT(*)                            AS line_item_count,
        SUM(net_amount)                     AS sum_net_amount,
        SUM(quantity)                       AS total_units,
        COUNT(DISTINCT product_service_code) AS distinct_services
    FROM {{ ref('ClaimItem') }}
    GROUP BY claim_id
),

-- Count diagnoses per claim
diag_counts AS (
    SELECT claim_id, COUNT(*) AS diagnosis_count
    FROM   {{ ref('ClaimDiagnosis') }}
    GROUP BY claim_id
),

-- Count procedures per claim
proc_counts AS (
    SELECT claim_id, COUNT(*) AS procedure_count
    FROM   {{ ref('ClaimProcedure') }}
    GROUP BY claim_id
),

-- Count care team members per claim
team_counts AS (
    SELECT claim_id, COUNT(DISTINCT provider_id) AS care_team_size
    FROM   {{ ref('ClaimCareTeam') }}
    GROUP BY claim_id
)

SELECT
    -- Claim keys
    c.claim_sk,
    c.claim_id,
    c.claim_status,
    c.claim_use,
    c.type_display                          AS claim_type,
    c.subtype_text                          AS claim_subtype,
    c.priority_display                      AS claim_priority,

    -- Patient
    c.patient_id,
    p.full_name                             AS patient_name,
    p.gender                                AS patient_gender,
    p.birth_date                            AS patient_dob,
    p.address_city                          AS patient_city,
    p.address_state                         AS patient_state,

    -- Provider
    c.provider_id,
    c.provider_display                      AS provider_name,

    -- Insurer (Organization)
    c.insurer_org_id,
    ins.insurer_name,
    ins.insurer_city,

    -- Facility
    c.facility_id,
    c.facility_display                      AS facility_name,

    -- Coverage
    c.coverage_id,
    cov.coverage_type,
    cov.network                             AS insurance_network,
    cov.plan_class,
    cov.payor_display,

    -- Dates
    CAST(c.claim_created_date   AS DATE)    AS claim_created_date,
    CAST(c.billable_period_start AS DATE)   AS service_start_date,
    CAST(c.billable_period_end   AS DATE)   AS service_end_date,
    DATEDIFF(
        DAY,
        CAST(c.billable_period_start AS DATE),
        CAST(c.billable_period_end   AS DATE)
    )                                       AS service_days,

    -- Financial measures
    c.total_amount                          AS claimed_total,
    c.total_currency,
    ISNULL(it.sum_net_amount, 0)            AS items_net_total,
    ISNULL(it.line_item_count, 0)           AS line_item_count,
    ISNULL(it.total_units, 0)               AS total_units_claimed,
    ISNULL(it.distinct_services, 0)         AS distinct_services,

    -- Clinical counts
    ISNULL(dc.diagnosis_count, 0)           AS diagnosis_count,
    ISNULL(pc.procedure_count, 0)           AS procedure_count,
    ISNULL(tc.care_team_size, 0)            AS care_team_size,

    -- Version info
    c.version_rank,
    {{ to_datetime2('c.meta_lastupdated') }}  AS claim_last_updated,
    {{ to_datetime2('GETUTCDATE()') }}        AS gold_loaded_at

FROM latest_claims              c
LEFT JOIN patients              p   ON c.patient_id    = p.patient_id
LEFT JOIN insurers              ins ON c.insurer_org_id = ins.org_id
LEFT JOIN coverage              cov ON c.coverage_id   = cov.coverage_id
LEFT JOIN item_totals           it  ON c.claim_id      = it.claim_id
LEFT JOIN diag_counts           dc  ON c.claim_id      = dc.claim_id
LEFT JOIN proc_counts           pc  ON c.claim_id      = pc.claim_id
LEFT JOIN team_counts           tc  ON c.claim_id      = tc.claim_id
