-- --------------------------------------------------------------------------
-- 5.  CLAIM CARE TEAM  –  helper view (providers per claim)
-- --------------------------------------------------------------------------
CREATE   VIEW Silver.fact_claim_care_team AS
SELECT
    entry_resource_id                                          AS claim_id,
    entry_resource_careteam_sequence                           AS care_team_sequence,
    REPLACE(entry_resource_careteam_provider_reference,
            'Practitioner/', '')                               AS provider_id,
    entry_resource_careteam_provider_display                   AS provider_display,
    entry_resource_careteam_provider_identifier_value          AS provider_identifier,
    entry_resource_careteam_role_coding_code                   AS role_code,
    entry_resource_careteam_role_coding_display                AS role_display,
    entry_resource_meta_lastupdated                            AS meta_lastupdated
FROM HackathonLh.dbo.Claim
WHERE entry_resource_id               IS NOT NULL
  AND entry_resource_careteam_sequence IS NOT NULL;

GO