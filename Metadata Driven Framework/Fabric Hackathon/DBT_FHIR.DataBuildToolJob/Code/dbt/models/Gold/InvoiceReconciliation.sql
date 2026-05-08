{{
    config(
        materialized = 'table',
        schema       = 'Gold'
    )
}}

/*
  gold_invoice_reconciliation
  ────────────────────────────
  One row per LATEST invoice version, joined to its source claim and patient.
  Surfaces claimed vs billed amounts for reconciliation / payment analytics.
  Price component breakdown (base charge, tax, discount, surcharge) is
  aggregated as pivot columns for easy BI consumption.
*/

WITH latest_invoices AS (
    SELECT * FROM {{ ref('Invoice') }}
),

latest_claims AS (
    SELECT claim_id, total_amount AS claimed_amount, total_currency,
           billable_period_start, billable_period_end,
           type_display AS claim_type, claim_status, insurer_org_id
    FROM   {{ ref('Claim') }}
),
patients AS (
    SELECT patient_id, full_name, gender, address_city, address_state
    FROM   {{ ref('Patient') }}
),

issuers AS (
    SELECT org_id, org_name AS issuer_org_name
    FROM   {{ ref('Organization') }} 
),

-- Pivot price components to columns
line_components AS (
    SELECT
        invoice_id,
        SUM(CASE WHEN price_component_type = 'base'        THEN price_component_amount ELSE 0 END) AS base_charge,
        SUM(CASE WHEN price_component_type = 'tax'         THEN price_component_amount ELSE 0 END) AS tax_amount,
        SUM(CASE WHEN price_component_type = 'discount'    THEN price_component_amount ELSE 0 END) AS discount_amount,
        SUM(CASE WHEN price_component_type = 'deduction'   THEN price_component_amount ELSE 0 END) AS deduction_amount,
        SUM(CASE WHEN price_component_type = 'surcharge'   THEN price_component_amount ELSE 0 END) AS surcharge_amount,
        SUM(CASE WHEN price_component_type = 'informational' THEN price_component_amount ELSE 0 END) AS informational_amount,
        COUNT(DISTINCT line_item_sequence)                                                          AS line_item_count
    FROM {{ ref('InvoiceLineItem') }}
    GROUP BY invoice_id
)

SELECT
    -- Invoice keys
    i.invoice_sk,
    i.invoice_id,
    i.invoice_status,
    CAST(i.invoice_date AS DATE)                    AS invoice_date,

    -- Linked claim
    i.source_claim_id                               AS claim_id,
    c.claim_type,
    c.claim_status,
    CAST(c.billable_period_start AS DATE)           AS service_start_date,
    CAST(c.billable_period_end   AS DATE)           AS service_end_date,
    c.claimed_amount,
    c.total_currency                                AS currency,

    -- Patient
    i.patient_id,
    p.full_name                                     AS patient_name,
    p.gender                                        AS patient_gender,
    p.address_city,
    p.address_state,

    -- Issuer (billing org)
    i.issuer_display,
    i.issuer_identifier_value,
    org.issuer_org_name,

    -- Invoice financial summary
    i.total_gross_amount,
    i.total_gross_currency,
    i.total_net_amount,
    i.total_net_currency,

    -- Price component breakdown
    ISNULL(lc.base_charge,          0)              AS base_charge,
    ISNULL(lc.tax_amount,           0)              AS tax_amount,
    ISNULL(lc.discount_amount,      0)              AS discount_amount,
    ISNULL(lc.deduction_amount,     0)              AS deduction_amount,
    ISNULL(lc.surcharge_amount,     0)              AS surcharge_amount,
    ISNULL(lc.informational_amount, 0)              AS informational_amount,
    ISNULL(lc.line_item_count,      0)              AS invoice_line_count,

    -- Reconciliation metrics
    ISNULL(c.claimed_amount, 0) - i.total_gross_amount
                                                    AS gross_variance,       -- +ve = underbilled
    ISNULL(c.claimed_amount, 0) - i.total_net_amount
                                                    AS net_variance,
    CASE
        WHEN ISNULL(c.claimed_amount, 0) = 0 THEN NULL
        ELSE ROUND(
            (i.total_net_amount / NULLIF(c.claimed_amount, 0)) * 100,
        2)
    END                                             AS net_to_claimed_pct,

    -- Notes
    i.note_text,

    -- Audit
    {{ to_datetime2('i.meta_lastupdated') }}        AS invoice_last_updated,
    {{ to_datetime2('GETUTCDATE()') }}        AS gold_loaded_at

FROM latest_invoices            i
LEFT JOIN latest_claims         c   ON i.source_claim_id = c.claim_id
LEFT JOIN patients              p   ON i.patient_id      = p.patient_id
LEFT JOIN issuers               org ON i.issuer_identifier_value = org.org_id
LEFT JOIN line_components       lc  ON i.invoice_id      = lc.invoice_id
