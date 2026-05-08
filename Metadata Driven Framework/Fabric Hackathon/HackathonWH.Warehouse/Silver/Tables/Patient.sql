CREATE TABLE [Silver].[Patient] (
    [patient_sk]           VARCHAR (64)   NULL,
    [patient_id]           VARCHAR (8000) NULL,
    [name_family]          VARCHAR (8000) NULL,
    [name_given]           VARCHAR (8000) NULL,
    [full_name]            VARCHAR (8000) NOT NULL,
    [gender]               VARCHAR (8000) NULL,
    [birth_date]           VARCHAR (8000) NULL,
    [address_use]          VARCHAR (8000) NULL,
    [address_line]         VARCHAR (8000) NULL,
    [address_city]         VARCHAR (8000) NULL,
    [address_state]        VARCHAR (8000) NULL,
    [address_postalcode]   VARCHAR (8000) NULL,
    [address_text]         VARCHAR (8000) NULL,
    [telecom_system]       VARCHAR (8000) NULL,
    [telecom_use]          VARCHAR (8000) NULL,
    [telecom_value]        VARCHAR (8000) NULL,
    [effective_start_date] DATETIME2 (0)  NULL,
    [effective_end_date]   DATETIME2 (0)  NULL,
    [is_current]           INT            NOT NULL,
    [meta_lastupdated]     DATETIME2 (0)  NULL,
    [meta_source]          VARCHAR (8000) NULL,
    [meta_versionid]       VARCHAR (8000) NULL,
    [resource_type]        VARCHAR (8000) NULL,
    [search_mode]          VARCHAR (8000) NULL,
    [full_url]             VARCHAR (8000) NULL,
    [Silver_loaded_at]     DATETIME2 (0)  NULL
);


GO