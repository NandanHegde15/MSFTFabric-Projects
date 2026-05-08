CREATE TABLE [dbo].[Date] (
    [Date]         DATE         NOT NULL,
    [MonthNumber]  INT          NULL,
    [YearNumber]   INT          NULL,
    [IsWeekend]    BIT          NULL,
    [DayValue]     INT          NULL,
    [DayName]      VARCHAR (20) NULL,
    [MonthEndDate] DATE         NULL
);


GO

ALTER TABLE [dbo].[Date]
    ADD CONSTRAINT [PK_Date_Date] PRIMARY KEY NONCLUSTERED ([Date] ASC) NOT ENFORCED;


GO