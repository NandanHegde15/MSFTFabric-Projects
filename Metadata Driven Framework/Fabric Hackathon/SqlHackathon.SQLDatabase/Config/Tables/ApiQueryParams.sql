CREATE TABLE [Config].[ApiQueryParams] (
    [QueryParamId]      INT            IDENTITY (1, 1) NOT NULL,
    [ApiConfigId]       INT            NOT NULL,
    [ParamName]         VARCHAR (200)  NOT NULL,
    [ParamValue]        VARCHAR (2000) NULL,
    [IsDynamic]         BIT            DEFAULT ((0)) NULL,
    [DynamicExpression] VARCHAR (2000) NULL,
    [IsActive]          BIT            DEFAULT ((1)) NULL,
    [SortOrder]         INT            DEFAULT ((1)) NULL,
    [CreatedDate]       DATETIME       DEFAULT (getdate()) NULL,
    [CreatedBy]         VARCHAR (250)  DEFAULT (suser_sname()) NULL,
    [ModifiedDate]      DATETIME       DEFAULT (getdate()) NULL,
    [ModifiedBy]        VARCHAR (250)  DEFAULT (suser_sname()) NULL,
    PRIMARY KEY CLUSTERED ([QueryParamId] ASC),
    CONSTRAINT [FK_ApiQueryParams_ApiConfig] FOREIGN KEY ([ApiConfigId]) REFERENCES [Config].[ApiConfig] ([ApiConfigId])
);


GO

