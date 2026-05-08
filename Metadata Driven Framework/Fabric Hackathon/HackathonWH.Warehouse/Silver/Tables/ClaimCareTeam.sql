CREATE TABLE [Silver].[ClaimCareTeam] (
    [claim_id]            VARCHAR (8000) NULL,
    [care_team_sequence]  BIGINT         NULL,
    [provider_id]         VARCHAR (8000) NULL,
    [provider_display]    VARCHAR (8000) NULL,
    [provider_identifier] VARCHAR (8000) NULL,
    [role_code]           VARCHAR (8000) NULL,
    [role_display]        VARCHAR (8000) NULL,
    [meta_lastupdated]    DATETIME2 (0)  NULL
);


GO