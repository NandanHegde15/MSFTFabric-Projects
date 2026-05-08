{{
    config(
        materialized = 'table',
        schema       = 'Gold'
    )
}}

/*
  gold_financial_summary_by_org
  ──────────────────────────────
  Aggregate financial KPIs grouped by Organization (insurer/facility),
  claim type, service month and state.
  Use for executive dashboards, cost-trend analysis and payer performance.
*/

WITH latest_claims AS (
    SELECT
        claim_id,
        patient_id,
        insurer_org_id,
        facility_id,
        claim_status,
        claim_use,
        type_code,
        type_display,
        subtype_code,
        total_amount,
        total_currency,
        CAST(claim_created_date    AS DATE) AS claim_date,
        CAST(billable_period_start AS DATE) AS service_start,
        CAST(billable_period_end   AS DATE) AS service_end,
        YEAR(CAST(claim_created_date  AS DATE))  AS claim_year,
        MONTH(CAST(claim_created_date AS DATE))  AS claim_month,
        DATEFROMPARTS(
            YEAR(CAST(claim_created_date  AS DATE)),
            MONTH(CAST(claim_created_date AS DATE)),
            1
        )                                   AS claim_month_start
    FROM {{ ref('Claim') }}
),

latest_invoices AS (
    SELECT
        invoice_id,
        source_claim_id,
        patient_id,
        invoice_status,
        total_gross_amount,
        total_net_amount,
        CAST(invoice_date AS DATE)          AS invoice_date
    FROM {{ ref('Invoice') }}
),

orgs AS (
    SELECT org_id, org_name, type_display AS org_type,
           address_state AS org_state, address_city AS org_city
    FROM   {{ ref('Organization') }}
),

patients AS (
    SELECT patient_id, address_state AS patient_state
    FROM   {{ ref('Patient') }}
),

-- Claim items totals per claim
item_totals AS (
    SELECT
        claim_id,
        SUM(net_amount)  AS items_net_total,
        SUM(quantity)    AS total_units,
        COUNT(*)         AS line_item_count
    FROM {{ ref('ClaimItem') }}
    GROUP BY claim_id
),

-- Join claims with invoices
claims_invoices AS (
    SELECT
        c.claim_id,
        c.patient_id,
        c.insurer_org_id,
        c.facility_id,
        c.claim_status,
        c.claim_use,
        c.type_code,
        c.type_display,
        c.total_amount                      AS claimed_amount,
        c.total_currency,
        c.claim_date,
        c.service_start,
        c.service_end,
        c.claim_year,
        c.claim_month,
        c.claim_month_start,
        ISNULL(it.items_net_total,  0)      AS items_net_total,
        ISNULL(it.total_units,      0)      AS total_units,
        ISNULL(it.line_item_count,  0)      AS line_item_count,
        inv.invoice_id,
        inv.invoice_status,
        ISNULL(inv.total_gross_amount, 0)   AS invoiced_gross,
        ISNULL(inv.total_net_amount,   0)   AS invoiced_net
    FROM latest_claims       c
    LEFT JOIN item_totals    it  ON c.claim_id      = it.claim_id
    LEFT JOIN latest_invoices inv ON c.claim_id     = inv.source_claim_id
)

SELECT
    -- Grouping dimensions
    COALESCE(ci.claim_month_start, CAST('2000-01-01' AS DATE))                   AS report_month,
    ci.claim_year,
    ci.claim_month,
    ci.claim_use,
    ci.type_display                         AS claim_type,

    -- Insurer org
    ci.insurer_org_id,
    ins.org_name                            AS insurer_name,
    ins.org_type                            AS insurer_type,
    ins.org_state                           AS insurer_state,

    -- Facility
    ci.facility_id,
    fac.org_name                            AS facility_name,
    fac.org_city                            AS facility_city,
    fac.org_state                           AS facility_state,

    -- Patient geography
    p.patient_state,

    -- Volume KPIs
    COUNT(DISTINCT ci.claim_id)             AS total_claims,
    COUNT(DISTINCT ci.patient_id)           AS unique_patients,
    COUNT(DISTINCT ci.invoice_id)           AS total_invoices,
    SUM(ci.line_item_count)                 AS total_line_items,
    SUM(ci.total_units)                     AS total_units_claimed,

    -- Claim status breakdown
    COUNT(DISTINCT CASE WHEN ci.claim_status = 'active'    THEN ci.claim_id END) AS active_claims,
    COUNT(DISTINCT CASE WHEN ci.claim_status = 'cancelled' THEN ci.claim_id END) AS cancelled_claims,
    COUNT(DISTINCT CASE WHEN ci.claim_status = 'entered-in-error' THEN ci.claim_id END) AS errored_claims,

    -- Financial KPIs
    SUM(ci.claimed_amount)                  AS total_claimed_amount,
    SUM(ci.items_net_total)                 AS total_items_net,
    SUM(ci.invoiced_gross)                  AS total_invoiced_gross,
    SUM(ci.invoiced_net)                    AS total_invoiced_net,
    SUM(ci.invoiced_gross - ci.invoiced_net) AS total_discounts_given,

    -- Efficiency metrics
    CASE
        WHEN SUM(ci.claimed_amount) = 0 THEN NULL
        ELSE ROUND(SUM(ci.invoiced_net) / NULLIF(SUM(ci.claimed_amount), 0) * 100, 2)
    END                                     AS collection_rate_pct,

    CASE
        WHEN COUNT(DISTINCT ci.claim_id) = 0 THEN NULL
        ELSE ROUND(SUM(ci.claimed_amount) / NULLIF(COUNT(DISTINCT ci.claim_id), 0), 2)
    END                                     AS avg_claim_amount,

    CASE
        WHEN COUNT(DISTINCT ci.invoice_id) = 0 THEN NULL
        ELSE ROUND(SUM(ci.invoiced_net) / NULLIF(COUNT(DISTINCT ci.invoice_id), 0), 2)
    END                                     AS avg_invoice_net,

    -- Audit
    {{ to_datetime2('GETUTCDATE()') }}        AS gold_loaded_at                              

FROM claims_invoices            ci
LEFT JOIN orgs                  ins ON ci.insurer_org_id = ins.org_id
LEFT JOIN orgs                  fac ON ci.facility_id    = fac.org_id
LEFT JOIN patients              p   ON ci.patient_id     = p.patient_id
GROUP BY
    ci.claim_month_start,
    ci.claim_year,
    ci.claim_month,
    ci.claim_use,
    ci.type_display,
    ci.insurer_org_id,
    ins.org_name,
    ins.org_type,
    ins.org_state,
    ci.facility_id,
    fac.org_name,
    fac.org_city,
    fac.org_state,
    p.patient_state
