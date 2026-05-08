-- Step 1: For each month in the [dbo].[Date] table, identify the last date of the month (MonthEndDate).
-- Step 2: For each MonthEndDate, check if it is a weekend (IsWeekend = 1).
-- Step 3: If MonthEndDate is a weekend, find the latest date before MonthEndDate in the same month that is not a weekend (IsWeekend = 0).
-- Step 4: If MonthEndDate is not a weekend, it is the last working day.
-- Step 5: For each year/month, return the last working day (not Saturday or Sunday).
-- Step 6: Create a view [dbo].[vw_LastWorkingDayOfMonth] with columns: [YearNumber], [MonthNumber], [LastWorkingDay].

CREATE   VIEW [Silver].[vwLastWorkingDayOfMonth] AS
WITH MonthEnds AS (
    -- Get distinct MonthEndDate, YearNumber, and MonthNumber for each month
    SELECT
        [MonthEndDate],
        [YearNumber],
        [MonthNumber]
    FROM [dbo].[Date]
    GROUP BY [MonthEndDate], [YearNumber], [MonthNumber]
),
LastWorkingDay AS (
    -- For each month, find the last working day (not Saturday or Sunday)
    SELECT
        me.[YearNumber],
        me.[MonthNumber],
        -- If MonthEndDate is not a weekend, use it; otherwise, find the latest non-weekend date before MonthEndDate in the same month
        MAX(d.[Date]) AS [LastWorkingDay]
    FROM MonthEnds me
    JOIN [dbo].[Date] d
        ON d.[YearNumber] = me.[YearNumber]
        AND d.[MonthNumber] = me.[MonthNumber]
        AND d.[Date] <= me.[MonthEndDate]
        AND d.[IsWeekend] = 0
    GROUP BY me.[YearNumber], me.[MonthNumber]
)
SELECT
    [YearNumber],
    [MonthNumber],
    [LastWorkingDay]
FROM LastWorkingDay;

GO