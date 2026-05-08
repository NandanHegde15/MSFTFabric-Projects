-- --------------------------------------------------------------------------
-- 4.  CLAIM PROCEDURE  –  helper view (normalised procedures per claim)
-- --------------------------------------------------------------------------
CREATE   VIEW Silver.fact_claim_procedure AS
SELECT
    entry_resource_id                                                    AS claim_id,
    entry_resource_procedure_sequence                                    AS procedure_sequence,
    entry_resource_procedure_procedurecodeableconcept_coding_code        AS procedure_code,
    entry_resource_procedure_procedurecodeableconcept_coding_display     AS procedure_display,
    entry_resource_procedure_procedurecodeableconcept_coding_system      AS procedure_system,
    REPLACE(entry_resource_procedure_procedurereference_reference,
            'Procedure/', '')                                            AS procedure_ref_id,
    entry_resource_procedure_date                                        AS procedure_date,
    entry_resource_meta_lastupdated                                      AS meta_lastupdated
FROM HackathonLh.dbo.Claim
WHERE entry_resource_id                IS NOT NULL
  AND entry_resource_procedure_sequence IS NOT NULL;

GO