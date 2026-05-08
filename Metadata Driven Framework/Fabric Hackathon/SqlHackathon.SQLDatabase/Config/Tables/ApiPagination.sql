CREATE TABLE [Config].[ApiPagination] (
    [PaginationId]    INT            IDENTITY (1, 1) NOT NULL,
    [ApiConfigId]     INT            NOT NULL,
    [PaginationType]  VARCHAR (50)   NOT NULL,
    [PaginationValue] VARCHAR (1000) NULL,
    [CreatedDate]     DATETIME       DEFAULT (getdate()) NULL,
    [CreatedBy]       VARCHAR (250)  DEFAULT (suser_sname()) NULL,
    [ModifiedDate]    DATETIME       DEFAULT (getdate()) NULL,
    [ModifiedBy]      VARCHAR (250)  DEFAULT (suser_sname()) NULL,
    PRIMARY KEY CLUSTERED ([PaginationId] ASC),
    CONSTRAINT [FK_ApiPagination_ApiConfig] FOREIGN KEY ([ApiConfigId]) REFERENCES [Config].[ApiConfig] ([ApiConfigId])
);


GO

