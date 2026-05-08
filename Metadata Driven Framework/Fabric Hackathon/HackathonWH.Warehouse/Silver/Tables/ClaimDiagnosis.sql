CREATE TABLE [Silver].[ClaimDiagnosis] (
    [claim_id]              VARCHAR (8000) NULL,
    [diagnosis_sequence]    BIGINT         NULL,
    [diagnosis_code]        VARCHAR (8000) NULL,
    [diagnosis_display]     VARCHAR (8000) NULL,
    [diagnosis_system]      VARCHAR (8000) NULL,
    [diagnosis_type_code]   VARCHAR (8000) NULL,
    [diagnosis_type_system] VARCHAR (8000) NULL,
    [condition_id]          VARCHAR (8000) NULL,
    [meta_lastupdated]      DATETIME2 (0)  NULL
);


GO