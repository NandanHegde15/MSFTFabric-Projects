CREATE TABLE [Config].[ApiConfig] (
    [ApiConfigId]             INT            IDENTITY (1, 1) NOT NULL,
    [ApiName]                 VARCHAR (200)  NOT NULL,
    [BaseUrl]                 VARCHAR (1000) NOT NULL,
    [RelativeUrl]             VARCHAR (1000) NULL,
    [HttpMethod]              VARCHAR (10)   DEFAULT ('GET') NULL,
    [SourceConnectionId]      VARCHAR (200)  NOT NULL,
    [AuthType]                VARCHAR (50)   NULL,
    [AuthHeader]              VARCHAR (500)  NULL,
    [AuthValue]               VARCHAR (2000) NULL,
    [Headers]                 VARCHAR (MAX)  NULL,
    [RequestBody]             VARCHAR (MAX)  NULL,
    [DestinationConnectionId] VARCHAR (200)  NOT NULL,
    [DestinationLakehouse]    VARCHAR (200)  NOT NULL,
    [DestinationPath]         VARCHAR (1000) NULL,
    [FileFormat]              VARCHAR (50)   DEFAULT ('json') NULL,
    [IsActive]                BIT            DEFAULT ((1)) NULL,
    [CreatedDate]             DATETIME       DEFAULT (getdate()) NULL,
    [CreatedBy]               VARCHAR (250)  DEFAULT (suser_sname()) NULL,
    [ModifiedDate]            DATETIME       DEFAULT (getdate()) NULL,
    [ModifiedBy]              VARCHAR (250)  DEFAULT (suser_sname()) NULL,
    [DestinationWorkspaceId]  VARCHAR (100)  NULL,
    [DestinationLakehouseId]  VARCHAR (100)  NULL,
    PRIMARY KEY CLUSTERED ([ApiConfigId] ASC)
);


GO

