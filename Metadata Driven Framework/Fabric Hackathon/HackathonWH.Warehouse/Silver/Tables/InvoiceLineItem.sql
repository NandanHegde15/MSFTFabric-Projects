CREATE TABLE [Silver].[InvoiceLineItem] (
    [invoice_line_sk]           VARCHAR (64)   NULL,
    [invoice_id]                VARCHAR (8000) NULL,
    [line_item_sequence]        BIGINT         NULL,
    [charge_item_id]            VARCHAR (8000) NULL,
    [charge_item_code]          VARCHAR (8000) NULL,
    [charge_item_system]        VARCHAR (8000) NULL,
    [price_component_type]      VARCHAR (8000) NULL,
    [price_component_code]      VARCHAR (8000) NULL,
    [price_component_system]    VARCHAR (8000) NULL,
    [price_component_amount]    FLOAT (53)     NULL,
    [price_component_currency]  VARCHAR (8000) NULL,
    [participant_role_code]     VARCHAR (8000) NULL,
    [participant_role_display]  VARCHAR (8000) NULL,
    [participant_actor_display] VARCHAR (8000) NULL,
    [participant_actor_id]      VARCHAR (8000) NULL,
    [participant_actor_type]    VARCHAR (8000) NULL,
    [meta_lastupdated]          DATETIME2 (0)  NULL,
    [silver_loaded_at]          DATETIME2 (0)  NULL
);


GO