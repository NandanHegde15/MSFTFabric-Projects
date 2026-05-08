CREATE TABLE [Silver].[ClaimProcedure] (
    [claim_id]           VARCHAR (8000) NULL,
    [procedure_sequence] BIGINT         NULL,
    [procedure_code]     VARCHAR (8000) NULL,
    [procedure_display]  VARCHAR (8000) NULL,
    [procedure_system]   VARCHAR (8000) NULL,
    [procedure_ref_id]   VARCHAR (8000) NULL,
    [procedure_date]     DATETIME2 (0)  NULL,
    [meta_lastupdated]   DATETIME2 (0)  NULL
);


GO