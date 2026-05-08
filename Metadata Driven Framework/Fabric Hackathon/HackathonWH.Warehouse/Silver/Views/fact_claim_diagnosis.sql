-- --------------------------------------------------------------------------
-- 3.  CLAIM DIAGNOSIS  –  helper view (normalised diagnosis codes per claim)
-- --------------------------------------------------------------------------
CREATE   VIEW Silver.fact_claim_diagnosis AS
SELECT
    entry_resource_id                                              AS claim_id,
    entry_resource_diagnosis_sequence                              AS diagnosis_sequence,
    entry_resource_diagnosis_diagnosiscodeableconcept_coding_code  AS diagnosis_code,
    entry_resource_diagnosis_diagnosiscodeableconcept_coding_display AS diagnosis_display,
    entry_resource_diagnosis_diagnosiscodeableconcept_coding_system AS diagnosis_system,
    entry_resource_diagnosis_type_coding_code                      AS diagnosis_type_code,
    entry_resource_diagnosis_type_coding_system                    AS diagnosis_type_system,
    REPLACE(entry_resource_diagnosis_diagnosisreference_reference,
            'Condition/', '')                                      AS condition_id,
    entry_resource_meta_lastupdated                                AS meta_lastupdated
FROM HackathonLh.dbo.Claim
WHERE entry_resource_id               IS NOT NULL
  AND entry_resource_diagnosis_sequence IS NOT NULL;

GO